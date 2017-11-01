package cache

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/BurntSushi/toml"
	mmap "github.com/edsrzf/mmap-go"
	"github.com/horazont/dragonstash/internal/backend"
)

const (
	ROOT_INODE_NAME = "root.inode"
	ROOT_STATE_NAME = "root.state"
	DIR_SUFFIX      = ".dir"
	DATA_SUFFIX     = ".data"
	LINK_SUFFIX     = ".link"
	BLOCKMAP_SUFFIX = ".blocks"
	BLOCK_SIZE      = 4096
)

const (
	block_prio_written   = iota
	block_prio_read      = iota
	block_prio_readahead = iota
)

type quotaMetadata struct {
	BlocksUsed  uint64 `toml:"blocks_used"`
	BlocksTotal uint64 `toml:"blocks_total"`
}

type metadata struct {
	Quota quotaMetadata `toml:"quota"`
}

type inode struct {
	name     string
	cache    dirCacheEntry
	children map[string]*inode
	parent   *inode
	file     *FileHandle
}

func (m *inode) Name() string {
	return m.name
}

func (m *inode) Stat() backend.FileStat {
	return &m.cache
}

func (m *inode) Mode() uint32 {
	return m.cache.Mode()
}

func inodeFromCacheEntry(entry *dirCacheEntry) *inode {
	node := &inode{
		name:     entry.Name(),
		cache:    *entry,
		children: nil,
	}
	return node
}

func loadDirFromReader(r io.Reader) (map[string]*inode, error) {
	cache := dirCache{}
	_, err := toml.DecodeReader(r, &cache)
	if err != nil {
		return nil, err
	}

	result := make(map[string]*inode)
	for _, entry := range cache.Entries {
		result[entry.Name()] = inodeFromCacheEntry(&entry)
	}

	return result, nil
}

func loadDirFromFile(path string) (map[string]*inode, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return loadDirFromReader(f)
}

func loadInodeFromReader(r io.Reader) (*inode, error) {
	entry := dirCacheEntry{}
	_, err := toml.DecodeReader(r, &entry)
	if err != nil {
		return nil, err
	}

	return inodeFromCacheEntry(&entry), nil
}

func loadInodeFromFile(path string) (*inode, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return loadInodeFromReader(f)
}

func (m *inode) path() string {
	parts := []string{m.name}
	for parent := m.parent; parent != nil; parent = parent.parent {
		parts = append(parts, parent.name)
	}

	// I don’t like to have to write an explicit reversal here, but this
	// seems to be how things are done in golang
	for i := len(parts)/2 - 1; i >= 0; i-- {
		opp := len(parts) - 1 - i
		parts[i], parts[opp] = parts[opp], parts[i]
	}

	return path.Join(parts...)
}

type dummyStat struct {
	backend.DefaultFileStat
}

func newDummyStat() *dummyStat {
	return &dummyStat{*backend.NewDefaultFileStat()}
}

func (m *dummyStat) Mode() uint32 {
	return syscall.S_IFDIR
}

type FileCache struct {
	lock          *sync.Mutex
	root_dir      string
	root_node     *inode
	metadata      metadata
	metadataStale bool
	staleInodes   map[*inode]bool
	fileHandles   map[string]*FileHandle
}

func NewFileCache(root string) *FileCache {
	root_path := filepath.Join(root, ROOT_INODE_NAME)
	root_node, _ := loadInodeFromFile(root_path)

	if root_node == nil {
		root_node = &inode{
			cache: dirCacheEntry{
				ModeV: syscall.S_IFDIR,
			},
		}
	}

	metadata := metadata{}
	f, err := os.Open(filepath.Join(root, ROOT_STATE_NAME))
	if err == nil {
		defer f.Close()
		toml.DecodeReader(f, &metadata)
	}

	return &FileCache{
		lock:        new(sync.Mutex),
		root_dir:    root,
		root_node:   root_node,
		metadata:    metadata,
		staleInodes: make(map[*inode]bool),
	}
}

func (m *FileCache) SetBlocksTotal(newTotal uint64) {
	m.metadata.Quota.BlocksTotal = newTotal
}

func (m *FileCache) BlockSize() int64 {
	return 4096
}

func (m *FileCache) inodeChildren(node *inode) (map[string]*inode, backend.Error) {
	if node.children != nil {
		return node.children, nil
	}
	if node.cache.Mode()&syscall.S_IFDIR == 0 {
		// is not a directory
		return nil, backend.WrapError(syscall.Errno(syscall.ENOTDIR))
	}
	path := m.getStoragePath(node.path(), DIR_SUFFIX)
	var err error
	node.children, err = loadDirFromFile(path)
	for _, child := range node.children {
		child.parent = node
	}
	if err != nil {
		// need to convert into I/O error: we can’t read the cache
		return nil, backend.NewBackendError(
			err.Error(),
			syscall.EIO,
		)
	}
	return node.children, backend.WrapError(err)
}

func (m *FileCache) getInode(path string) (*inode, backend.Error) {
	log.Printf("getInode: %#v", path)
	if path == "/" || path == "" {
		return m.root_node, nil
	}
	if strings.HasPrefix(path, "/") {
		path = path[1:]
	}
	path = filepath.Clean(path)
	log.Printf("getInode: -> %#v", path)
	parts := strings.Split(path, string(filepath.Separator))
	log.Printf("getInode: -> %#v", parts)
	node := m.root_node
	for _, part := range parts {
		children, err := m.inodeChildren(node)
		if err != nil {
			log.Printf("getInode: %#v: searching for %#v: cannot load"+
				" children: %s", path, part, err)
			return nil, err
		}
		log.Printf("getInode: %#v: searching for %#v: found %d children", path, part, len(children))
		child, ok := children[part]
		if !ok {
			log.Printf("getInode: %#v: searching for %#v: ENOENT", path, part)
			return nil, backend.WrapError(
				syscall.Errno(syscall.ENOENT),
			)
		}
		node = child
	}
	return node, nil
}

func (m *FileCache) getInodeAutofill(
	path string,
	fs backend.FileSystem,
) (*inode, backend.Error) {
	// idea: move upwards through the tree and create inodes as needed
	if path == "/" || path == "" {
		return m.root_node, nil
	}
	if strings.HasPrefix(path, "/") {
		path = path[1:]
	}
	path = filepath.Clean(path)
	log.Printf("getInodeAutofill: -> %#v", path)
	parts := strings.Split(path, string(filepath.Separator))

	node := m.root_node
	path_so_far := ""
	for _, part := range parts {
		children, err := m.inodeChildren(node)
		if err != nil {
			log.Printf("getInode: %#v: asking fs for %#v", path, part)
			// try to fill from fs
			entries, err := fs.OpenDir(path_so_far)
			if err != nil {
				log.Printf(
					"getInodeAutofill: %#v: fs could not provide %#v: %s",
					path,
					part,
					err,
				)
				return nil, err
			}
			m.fillInode(node, entries)
			children = node.children
		}

		child, ok := children[part]
		if !ok {
			log.Printf("getInodeAutofill: %#v: searching for %#v: ENOENT", path, part)
			return nil, backend.WrapError(
				syscall.Errno(syscall.ENOENT),
			)
		}

		path_so_far = fs.Join(path_so_far, part)
		node = child
	}

	return node, nil
}

func (m *FileCache) fillInode(node *inode, entries []backend.DirEntry) {
	if node.Stat().Mode()&syscall.S_IFDIR == 0 {
		panic("attempt to add children to a non-directory inode")
	}

	if node.children != nil {
		// we need to merge
		unseenmap := make(map[string]*inode)
		for k, child := range node.children {
			unseenmap[k] = child
		}

		log.Printf("FileCache: synchronizing %d items into dir with %d items",
			len(entries),
			node.children)

		for _, entry := range entries {
			name := entry.Name()
			delete(unseenmap, name)
			child, ok := node.children[name]
			if ok {
				updateStatToCache(entry.Stat(), &child.cache)
			} else {
				new_inode := &inode{
					name:   name,
					parent: node,
				}
				fullDirEntryToCache(name, entry.Stat(), &new_inode.cache)
				node.children[name] = new_inode
			}
		}

		log.Printf("FileCache: removing %d stale items from dir", len(unseenmap))

		for k, child := range unseenmap {
			m.deleteRecursively(child, child.path())
			delete(node.children, k)
		}
	} else {
		// simply fill
		node.children = make(map[string]*inode)
		for _, entry := range entries {
			new_inode := &inode{
				name:   entry.Name(),
				parent: node,
			}
			fullDirEntryToCache(entry.Name(), entry.Stat(), &new_inode.cache)
			node.children[entry.Name()] = new_inode
		}
	}
	m.markInodeStale(node)
}

func (m *FileCache) getStoragePath(path string, suffix string) string {
	hash := sha256.Sum256([]byte(path))
	encoded := base64.URLEncoding.EncodeToString(hash[:])
	p1 := encoded[:3]
	p2 := encoded[3:6]
	p3 := encoded[6:]
	return strings.TrimRight(filepath.Join(m.root_dir, p1, p2, p3), "=") + suffix
}

func (m *FileCache) openStorage(
	storage_path string,
	write bool,
) (f *safeFile, err error) {
	if write {
		err := m.createStorageDirs(storage_path)
		if err != nil {
			log.Printf("FileCache: could not create cache directories: %s", err)
			return nil, err
		}
		f, err = CreateSafe(storage_path)
	} else {
		f, err = OpenSafe(storage_path)
	}

	if err != nil {
		return nil, err
	}

	return f, nil
}

func (m *FileCache) createStorageDirs(storage_path string) error {
	return os.MkdirAll(filepath.Dir(storage_path), 0700)
}

func (m *FileCache) PutDir(
	path string,
	fs backend.FileSystem,
	entries []backend.DirEntry,
) {
	m.lock.Lock()
	defer m.lock.Unlock()

	parent_inode, err := m.getInodeAutofill(path, fs)
	if err != nil {
		// parent not in cache yet? -> discard
		// FIXME: in the future we’d want to create a cache entry
		// nevertheless, including for parent directories
		return
	}

	// try to load the children from disk
	_, _ = m.inodeChildren(parent_inode)
	m.fillInode(parent_inode, entries)

	log.Printf("PutDir: writing inode %s %v",
		parent_inode.name,
		parent_inode)

	// log.Printf("FileCache: request to store %d entries for %s", len(entries), path)
	// cachepath := m.getStoragePath(path)
	// log.Printf("FileCache: storage for %s is at %s", path, cachepath)
	// cache := dirCache{}
	// cache.Entries = dirEntriesToCache(path, fs, entries)
	// err := os.MkdirAll(filepath.Dir(cachepath), 0700)
	// if err != nil {
	// 	log.Printf("FileCache: could not create cache directories: %s", err)
	// 	return
	// }
	// dest, err := os.Create(cachepath)
	// if err != nil {
	// 	log.Printf("FileCache: could not create cache file: %s", err)
	// 	return
	// }
	// encoder := toml.NewEncoder(bufio.NewWriter(dest))
	// encoder.Encode(&cache)

	m.writeback()
}

func (m *FileCache) writeDirectory(node *inode) {
	if node.Stat().Mode()&syscall.S_IFDIR == 0 {
		panic(fmt.Sprintf(
			"attempt to write non-directory as directory: %s",
			node.name))
	}

	log.Printf("writeDirectory: writing dir %#v", node.name)

	storage_path := m.getStoragePath(node.path(), DIR_SUFFIX)
	f, err := m.openStorage(storage_path, true)
	if err != nil {
		log.Printf(
			"writeDirectory: failed to open inode data for writing: %s",
			err,
		)
		return
	}
	defer f.Close()

	cache := dirCache{}
	cache.Entries = make([]dirCacheEntry, len(node.children))
	var i = 0
	for name, entry := range node.children {
		cache.Entries[i].NameV = name
		cache.Entries[i] = entry.cache
		i += 1
	}

	encoder := toml.NewEncoder(f)
	err = encoder.Encode(&cache)
	if err != nil {
		log.Printf(
			"writeDirectory: failed to write: %s\n",
			err,
		)
		os.Remove(storage_path)
	}
}

func (m *FileCache) writeRootInode() {
	storage_path := filepath.Join(m.root_dir, ROOT_INODE_NAME)
	f, err := CreateSafe(storage_path)
	if err != nil {
		log.Printf(
			"writeRootInode: failed to open inode data for writing"+
				": %s\n",
			err,
		)
		return
	}

	stat := m.root_node.Stat()
	entry := dirCacheEntry{
		NameV:  "",
		ModeV:  stat.Mode(),
		MtimeV: stat.Mtime(),
		AtimeV: stat.Atime(),
		CtimeV: stat.Ctime(),
		SizeV:  stat.Size(),
		UidV:   stat.OwnerUID(),
		GidV:   stat.OwnerGID(),
	}

	encoder := toml.NewEncoder(f)
	err = encoder.Encode(&entry)
	if err != nil {
		log.Printf(
			"writeDirectory: failed to write: %s\n",
			err,
		)
		f.Abort()
		return
	}
	f.Close()
}

func (m *FileCache) writeMetadata() {
	storage_path := filepath.Join(m.root_dir, ROOT_STATE_NAME)
	f, err := CreateSafe(storage_path)
	if err != nil {
		log.Printf(
			"writeMetadata: failed to open inode data for writing"+
				": %s\n",
			err,
		)
		return
	}

	encoder := toml.NewEncoder(f)
	err = encoder.Encode(&m.metadata)
	if err != nil {
		log.Printf(
			"writeMetadata: failed to write: %s\n",
			err,
		)
		f.Abort()
		return
	}
	f.Close()
}

func (m *FileCache) FetchDir(path string) ([]backend.DirEntry, backend.Error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	inode, err := m.getInode(path)
	if err != nil {
		return nil, err
	}

	log.Printf("FetchDir: inode = %v", inode)

	children, err := m.inodeChildren(inode)
	if err != nil {
		return nil, err
	}

	result := make([]backend.DirEntry, len(children))
	var i = 0
	for _, child := range children {
		result[i] = child
		i += 1
	}

	return result, nil
}

func (m *FileCache) FetchAttr(path string) (backend.FileStat, backend.Error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	inode, err := m.getInode(path)
	if err != nil {
		return nil, err
	}

	return inode.Stat(), nil
}

func (m *FileCache) PutAttr(path string, stat backend.FileStat) {
	m.lock.Lock()
	defer m.lock.Unlock()

	inode, err := m.getInode(path)
	if err != nil {
		log.Printf("PutAttr: getInode failed for %#v: %s", path, err)
		return
	}
	updateStatToCache(stat, &inode.cache)

	if path == "" {
		// root inode
		m.markInodeStale(inode)
		m.writeback()
	}
}

func (m *FileCache) PutLink(path string, dest string) {
	storage_path := m.getStoragePath(path, LINK_SUFFIX)
	f, err := m.openStorage(storage_path, true)
	if err != nil {
		return
	}

	_, err = f.WriteString(dest)
	if err != nil {
		f.Abort()
		return
	}
	f.Close()
}

func (m *FileCache) FetchLink(path string) (string, backend.Error) {
	f, err := m.openStorage(m.getStoragePath(path, LINK_SUFFIX), false)
	if err != nil {
		return "", backend.WrapError(syscall.Errno(syscall.EIO))
	}
	defer f.Close()

	dest := make([]byte, 0, 256)
	part := make([]byte, 256)
	var n int
	for n, err = f.Read(part); n > 0; n, err = f.Read(part) {
		dest = append(dest, part[:n]...)
	}

	if err != nil && err != io.EOF {
		return "", backend.WrapError(syscall.Errno(syscall.EIO))
	}

	return string(dest), nil
}

func (m *FileCache) deleteRecursively(node *inode, path string) {
	log.Printf("FileCache: deleteRecursively: at %#v, inode %v", path, node)

	children, _ := m.inodeChildren(node)
	if children != nil {
		for _, child := range children {
			m.deleteRecursively(
				child,
				filepath.Join(path, child.name),
			)
		}
	}

	// FIXME: delete file metadata
	mode := node.Mode()
	storage_path_base := m.getStoragePath(path, "")
	if mode&syscall.S_IFDIR != 0 {
		// is a directory, delete directory
		os.Remove(storage_path_base + DIR_SUFFIX)
	} else if mode&syscall.S_IFLNK != 0 {
		os.Remove(storage_path_base + LINK_SUFFIX)
	}

	delete(m.staleInodes, node)
}

func (m *FileCache) Delete(path string) {
	log.Printf("FileCache: deleting %#v recursively", path)

	m.lock.Lock()
	defer m.lock.Unlock()

	node, err := m.getInode(path)
	if err != nil {
		// appears to not be cached
		return
	}

	m.deleteRecursively(node, path)
	delete(node.parent.children, node.name)
	m.markInodeStale(node.parent)
	m.writeback()
}

func (m *FileCache) writeback() {
	log.Printf(
		"writeback: %d stale inodes; metadata stale: %s",
		len(m.staleInodes),
		m.metadataStale,
	)

	if m.metadataStale {
		m.writeMetadata()
	}

	for node, _ := range m.staleInodes {
		if node == m.root_node {
			m.writeRootInode()
		}
		if node.Mode()&syscall.S_IFDIR != 0 {
			m.writeDirectory(node)
		}
		delete(m.staleInodes, node)
	}
}

func (m *FileCache) markInodeStale(node *inode) {
	m.staleInodes[node] = true
}

func (m *FileCache) OpenForStore(
	path string,
	mtime uint64,
	size uint64,
) (CachedFileHandle, backend.Error) {
	storage_path_base := m.getStoragePath(path, "")
	m.createStorageDirs(storage_path_base)

	m.lock.Lock()
	defer m.lock.Unlock()

	node, err := m.getInode(path)
	if err != nil {
		return nil, err
	}

	if node.file != nil {
		return node.file, nil
	}

	data_storage_path := storage_path_base + DATA_SUFFIX
	blockmap_storage_path := storage_path_base + BLOCKMAP_SUFFIX

	dataf, oserr := os.OpenFile(
		data_storage_path,
		os.O_CREATE|os.O_RDWR,
		0600,
	)
	if oserr != nil {
		return nil, backend.NewBackendError(
			oserr.Error(),
			syscall.EIO,
		)
	}
	blockmapf, oserr := os.OpenFile(
		blockmap_storage_path,
		os.O_CREATE|os.O_RDWR,
		0600,
	)
	if oserr != nil {
		dataf.Close()
		return nil, backend.NewBackendError(
			oserr.Error(),
			syscall.EIO,
		)
	}

	return &FileHandle{
		path:          path,
		cache:         m,
		lock:          new(sync.Mutex),
		data_file:     dataf,
		blockmap_file: blockmapf,
		blockmap:      nil,
		refcnt:        1,
	}, nil
}

func (m *FileCache) closeHandle(handle *FileHandle) {
	if m.fileHandles[handle.path] != handle {
		panic("inconsistent internal state: handle isn’t at expected path")
	}

	delete(m.fileHandles, handle.path)
}

func (m *FileCache) requestBlocks(path string, nblocks uint64, priority int) bool {
	// request a number of blocks, return true if request passes by quota
	// priority is ignored for now, but in the future, it should behave like this:
	//
	//     readahead -> no blocks are ever evicted for this
	//     read, written -> apply usual eviction
	//
	// (written may get different semantics based on eviction pattern)
	log.Printf("requestBlocks: request for %d blocks", nblocks)
	if nblocks == 0 {
		return true
	}

	free_blocks := m.metadata.Quota.BlocksTotal - m.metadata.Quota.BlocksUsed
	if free_blocks < nblocks {
		return false
	}

	inode, err := m.getInode(path)
	if err != nil {
		return false
	}

	inode.cache.BlocksV += nblocks
	m.markInodeStale(inode.parent)
	m.metadata.Quota.BlocksUsed += nblocks
	m.metadataStale = true
	m.writeback()

	return true
}

type FileHandle struct {
	path          string
	cache         *FileCache
	lock          *sync.Mutex
	data_file     *os.File
	blockmap_file *os.File
	blockmap      mmap.MMap
	refcnt        uint64
}

func (m *FileHandle) ensureBlockmapMapped() {
	if m.blockmap != nil {
		return
	}

	blockmap, err := mmap.Map(m.blockmap_file, mmap.RDWR, 0)
	if err != nil {
		panic("failed to mmap blockmap!")
	}
	m.blockmap = blockmap
}

func (m *FileHandle) ensureBlockmapSize(last int64) {
	if m.blockmap != nil && int64(len(m.blockmap)) >= last {
		// large enough
		return
	}

	m.truncateAndRemap(last + 1)
}

func (m *FileHandle) truncateAndRemap(size int64) {
	// ensure that changes are written to disk first
	m.data_file.Sync()
	m.blockmap.Flush()

	m.blockmap.Unmap()
	m.blockmap = nil
	m.blockmap_file.Truncate(size)

	m.ensureBlockmapMapped()

	if int64(len(m.blockmap)) != size {
		panic("failed to resize blockmap!")
	}

}

func (m *FileHandle) truncateBlockmap(last int64) {
	if m.blockmap != nil && int64(len(m.blockmap)) == last+1 {
		// large enough
		return
	}

	m.truncateAndRemap(last + 1)
}

func (m *FileHandle) PutReadData(data []byte, position int64, at_eof bool) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if position%BLOCK_SIZE != 0 || (len(data)%BLOCK_SIZE != 0 && !at_eof) {
		panic("read is not aligned to block size")
	}

	nblocks := (len(data) + BLOCK_SIZE - 1) / BLOCK_SIZE
	firstBlock := position / BLOCK_SIZE
	lastBlock := firstBlock + int64(nblocks) - 1

	var newBlocks int64 = 0

	lastBlockmapBlock := int64(len(m.blockmap) - 1)
	lastIterBlock := lastBlock
	if lastIterBlock > lastBlockmapBlock {
		newBlocks += lastIterBlock - lastBlockmapBlock
		lastIterBlock = lastBlockmapBlock
	}
	for i := firstBlock; i <= lastIterBlock; i += 1 {
		if m.blockmap[i] == 0 {
			newBlocks += 1
		}
	}

	if newBlocks > 0 && !m.cache.requestBlocks(m.path, uint64(newBlocks), block_prio_read) {
		log.Printf("blocks rejected by quota management, not writing")
		return
	}

	m.ensureBlockmapSize(lastBlock)

	_, err := m.data_file.WriteAt(data, position)
	if err != nil {
		log.Printf("failed to write to cache file: %s", err)
		return
	}

	if at_eof {
		m.data_file.Truncate(position + int64(len(data)))
		m.truncateBlockmap(lastBlock)
	}

	m.data_file.Sync()

	for i := firstBlock; i <= lastBlock; i += 1 {
		m.blockmap[i] = 1
	}

	m.blockmap.Flush()
}

func truncateRead(position int64, length int64, lastReadableBlock int64) int64 {
	lastByte := position + length - 1
	lastReadableByte := (lastReadableBlock+1)*4096 - 1

	toOmit := lastByte - lastReadableByte
	return length - toOmit
}

func (m *FileHandle) ReadData(data []byte, position int64) (int, backend.Error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	firstBlock := position / BLOCK_SIZE
	nblocks := (len(data) + BLOCK_SIZE - 1) / BLOCK_SIZE
	lastBlock := firstBlock + int64(nblocks) - 1

	readLength := len(data)

	m.ensureBlockmapMapped()

	log.Printf("ReadData: firstBlock=%d, nblocks=%d lastBlock=%d readLength=%d",
		firstBlock,
		nblocks,
		lastBlock,
		readLength)

	lastReadableBlock := firstBlock
	lastCheckableBlock := lastBlock
	if int64(len(m.blockmap)) <= lastCheckableBlock {
		lastCheckableBlock = int64(len(m.blockmap) - 1)
	}
	if lastReadableBlock > lastCheckableBlock {
		// read is definitely out of bounds, no need to check blockmap
		lastReadableBlock = firstBlock - 1
	} else {
		for ; lastReadableBlock <= lastCheckableBlock; lastReadableBlock += 1 {
			if m.blockmap[lastReadableBlock] == 0 {
				lastReadableBlock -= 1
				break
			}
		}
	}

	truncated := false
	if lastReadableBlock < lastBlock {
		// bad, we don’t have the requested data
		// truncate read and return EIO
		readLength = int(truncateRead(position, int64(len(data)),
			lastReadableBlock))
		truncated = true
	}

	log.Printf("ReadData: lastReadableBlock=%d lastCheckableBlock=%d truncated=%s",
		lastReadableBlock,
		lastCheckableBlock,
		truncated)

	n, err := m.data_file.ReadAt(data[:readLength], position)
	if err != io.EOF && err != nil {
		return n, backend.WrapError(err)
	} else if truncated {
		return n, backend.WrapError(syscall.EIO)
	} else {
		return n, nil
	}
}

func (m *FileHandle) Close() {
	m.lock.Lock()
	defer m.lock.Unlock()

	log.Printf("decreasing refcount on file handle %#v", m)

	m.refcnt -= 1
	if m.refcnt == 0 {
		m.cache.closeHandle(m)
	}
}

func (m *FileHandle) purge() {
}
