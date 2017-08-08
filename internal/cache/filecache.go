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
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/horazont/dragonstash/internal/backend"
)

const (
	ROOT_INODE_NAME = "root.inode"
	DIR_SUFFIX      = ".dir"
	FILE_SUFFIX     = ".file"
	LINK_SUFFIX     = ".link"
)

type inode struct {
	name     string
	stat     backend.FileStat
	cache    dirCacheEntry
	children map[string]*inode
	parent   *inode
}

func (m *inode) Name() string {
	return m.name
}

func (m *inode) Stat() backend.FileStat {
	return m.stat
}

func (m *inode) Mode() uint32 {
	return m.stat.Mode()
}

func inodeFromCacheEntry(entry *dirCacheEntry) *inode {
	node := &inode{
		name:     entry.Name(),
		cache:    *entry,
		children: nil,
	}
	node.stat = &node.cache
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
	root_dir  string
	root_node *inode
}

func NewFileCache(root string) *FileCache {
	root_path := filepath.Join(root, ROOT_INODE_NAME)
	root_node, _ := loadInodeFromFile(root_path)

	if root_node == nil {
		root_node = &inode{
			stat: newDummyStat(),
		}
	}

	return &FileCache{
		root_dir:  root,
		root_node: root_node,
	}
}

func (m *FileCache) inodeChildren(node *inode) (map[string]*inode, backend.Error) {
	if node.children != nil {
		return node.children, nil
	}
	if node.stat.Mode()&syscall.S_IFDIR == 0 {
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
				child.stat = entry.Stat()
			} else {
				node.children[name] = &inode{
					name:   name,
					stat:   entry.Stat(),
					parent: node,
				}
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
			node.children[entry.Name()] = &inode{
				name:   entry.Name(),
				stat:   entry.Stat(),
				parent: node,
			}
		}
	}
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
) (f *os.File, err error) {
	if write {
		err = os.MkdirAll(filepath.Dir(storage_path), 0700)
		if err != nil {
			log.Printf("FileCache: could not create cache directories: %s", err)
			return nil, err
		}
		f, err = os.Create(storage_path)
	} else {
		f, err = os.Open(storage_path)
	}

	if err != nil {
		return nil, err
	}

	return f, nil
}

func (m *FileCache) PutDir(
	path string,
	fs backend.FileSystem,
	entries []backend.DirEntry,
) {
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
	m.writeDirectory(parent_inode)

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
		fullDirEntryToCache(name, entry.Stat(), &cache.Entries[i])
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
	f, err := os.Create(storage_path)
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
		os.Remove(storage_path)
	}
}

func (m *FileCache) FetchDir(path string) ([]backend.DirEntry, backend.Error) {
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

func (m *FileCache) DelDir(path string) {
	log.Printf("FileCache: request to delete cache for %s", path)
}

func (m *FileCache) FetchAttr(path string) (backend.FileStat, backend.Error) {
	inode, err := m.getInode(path)
	if err != nil {
		return nil, err
	}

	return inode.Stat(), nil
}

func (m *FileCache) PutAttr(path string, stat backend.FileStat) {
	inode, err := m.getInode(path)
	if err != nil {
		log.Printf("PutAttr: getInode failed for %#v: %s", path, err)
		return
	}
	inode.stat = stat

	if path == "" {
		// root inode
		m.writeRootInode()
	}
}

func (m *FileCache) PutLink(path string, dest string) {
	storage_path := m.getStoragePath(path, LINK_SUFFIX)
	f, err := m.openStorage(storage_path, true)
	if err != nil {
		return
	}
	defer f.Close()

	_, err = f.WriteString(dest)
	if err != nil {
		os.Remove(storage_path)
	}
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
}

func (m *FileCache) Delete(path string) {
	log.Printf("FileCache: deleting %#v recursively", path)

	node, err := m.getInode(path)
	if err != nil {
		// appears to not be cached
		return
	}

	m.deleteRecursively(node, path)
	delete(node.parent.children, node.name)
}
