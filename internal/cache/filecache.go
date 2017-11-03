package cache

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/horazont/dragonstash/internal/backend"
)

const (
	BLOCK_SIZE = 4096
	fmt_REG    = 1
	fmt_DIR    = 2
	fmt_LNK    = 3
)

var (
	ErrUnsupportedInode = errors.New("unsupported inode type")
)

func normalizePath(path string) string {
	if path == "/" {
		return ""
	}
	if len(path) > 0 && path[0] != '/' {
		path = "/" + path
	}
	return path
}

type inode interface {
	mutex() *sync.Mutex
	attr() *dirCacheEntry
	writeWithHeader(dest io.Writer) error
}

type regularInodeData struct {
	Attrib dirCacheEntry `toml:"attrib"`
}

type linkInodeData struct {
	Attrib   dirCacheEntry `toml:"attrib"`
	LinkDest string        `toml:"link_dest"`
}

type dirInodeData struct {
	Attrib   dirCacheEntry `toml:"attrib"`
	Children []string      `toml:"children"`
}

func readRegularInode(src io.Reader) (inode inode, err error) {
	data := regularInodeData{}
	_, err = toml.DecodeReader(src, &data)
	if err != nil {
		return nil, err
	}

	inode = &fileInode{
		inodeImpl: inodeImpl{
			_attr: data.Attrib,
		},
	}

	return inode, nil
}

func readDirectoryInode(src io.Reader) (inode inode, err error) {
	data := dirInodeData{}
	_, err = toml.DecodeReader(src, &data)
	if err != nil {
		return nil, err
	}

	inode = &dirInode{
		inodeImpl: inodeImpl{
			_attr: data.Attrib,
		},
		children: data.Children,
	}

	return inode, nil
}

func readLinkInode(src io.Reader) (inode inode, err error) {
	data := linkInodeData{}
	_, err = toml.DecodeReader(src, &data)
	if err != nil {
		return nil, err
	}

	inode = &linkInode{
		inodeImpl: inodeImpl{
			_attr: data.Attrib,
		},
		dest: data.LinkDest,
	}

	return inode, nil
}

func ReadInodeWithHeader(src io.Reader) (inode, error) {
	var format_header uint32
	err := binary.Read(src, binary.LittleEndian, &format_header)
	if err != nil {
		return nil, err
	}

	switch format_header {
	case fmt_REG:
		return readRegularInode(src)
	case fmt_DIR:
		return readDirectoryInode(src)
	case fmt_LNK:
		return readLinkInode(src)
	}

	return nil, ErrUnsupportedInode
}

type inodeImpl struct {
	_mutex sync.Mutex
	_attr  dirCacheEntry
}

func (m *inodeImpl) mutex() *sync.Mutex {
	return &m._mutex
}

func (m *inodeImpl) attr() *dirCacheEntry {
	return &m._attr
}

type fileInode struct {
	inodeImpl
	handle *fileCacheFile
}

func writeTomlWithHeader(dest io.Writer, format uint32, v interface{}) error {
	err := binary.Write(dest, binary.LittleEndian, &format)
	if err != nil {
		return err
	}
	encoder := toml.NewEncoder(dest)
	return encoder.Encode(v)
}

func (m *fileInode) writeWithHeader(dest io.Writer) error {
	return writeTomlWithHeader(dest, fmt_REG, &regularInodeData{
		Attrib: m._attr,
	})
}

type dirInode struct {
	inodeImpl
	children []string
}

func (m *dirInode) writeWithHeader(dest io.Writer) error {
	return writeTomlWithHeader(dest, fmt_DIR, &dirInodeData{
		Attrib:   m._attr,
		Children: m.children,
	})
}

type linkInode struct {
	inodeImpl
	dest string
}

func (m *linkInode) writeWithHeader(dest io.Writer) error {
	return writeTomlWithHeader(dest, fmt_LNK, &linkInodeData{
		Attrib:   m._attr,
		LinkDest: m.dest,
	})
}

type FileCache struct {
	lock        *sync.Mutex
	root_dir    string
	inodes      map[string]inode
	quota       quotaInfo
	dirtyInodes map[inode]bool
}

type fileCacheFile struct {
	// The inode of the file (also contains the lock)
	inode *inode
	// How many open descriptors there are. If this drops to zero, the file
	// may be evicted from the in-memory cache
	refcnt uint64
}

func NewFileCache(root_dir string) *FileCache {
	return &FileCache{
		lock:        new(sync.Mutex),
		root_dir:    root_dir,
		inodes:      make(map[string]inode),
		dirtyInodes: make(map[inode]bool),
	}
}

func (m *FileCache) markInodeDirty(node inode) {
	m.dirtyInodes[node] = true
}

func (m *FileCache) writebackInode(node inode) {
	path := m.getStoragePath(node.attr().NameV, ".inode")
	os.MkdirAll(filepath.Dir(path), 0700)
	log.Printf("writing inode for path %s to %s", node.attr().NameV, path)
	file, err := CreateSafe(path)
	if err != nil {
		log.Printf("failed to open for inode writing: %s", err)
		return
	}
	defer file.Abort()

	err = node.writeWithHeader(file)
	if err != nil {
		log.Printf("failed to write inode: %s", err)
		return
	}

	err = file.Close()
	if err != nil {
		log.Printf("failed to finish writing inode: %s", err)
	}

	delete(m.dirtyInodes, node)
}

func (m *FileCache) writeback() {
	for inode := range m.dirtyInodes {
		func() {
			inode.mutex().Lock()
			defer inode.mutex().Unlock()
			m.writebackInode(inode)
		}()
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

// Obtain the inode for a path
func (m *FileCache) getInode(path string) (inode, backend.Error) {
	// first try to load the inode from the map
	inode, ok := m.inodes[path]
	if ok {
		return inode, nil
	}

	// if that doesn’t work, try to load the inode from the fs
	backend_path := m.getStoragePath(path, ".inode")
	log.Printf("trying to load inode for path %s from %s", path, backend_path)
	file, err := os.Open(backend_path)
	if err != nil {
		log.Printf("failed to open inode: %s", err)
		return nil, backend.WrapError(syscall.EIO)
	}

	inode, err = ReadInodeWithHeader(file)
	if err != nil {
		log.Printf("failed to decode inode: %s", err)
		return nil, backend.WrapError(syscall.EIO)
	}

	inode.attr().NameV = path

	m.inodes[path] = inode
	m.markInodeDirty(inode)

	return inode, nil
}

func (m *FileCache) requireInode(path string, format uint32) inode {
	inode, ok := m.inodes[path]
	if ok && inode.attr().Mode()&syscall.S_IFMT == format {
		// return existing inode if mode matches
		return inode
	} else {
		// TODO: clean up old inode properly
	}

	switch format {
	case syscall.S_IFDIR:
		inode = &dirInode{}
	case syscall.S_IFREG:
		inode = &fileInode{}
	case syscall.S_IFLNK:
		inode = &linkInode{}
	default:
		panic("attempt to create an unsupported inode type")
	}
	inode.attr().NameV = path
	// force the mode to be correct
	inode.attr().ModeV = (inode.attr().ModeV &^ syscall.S_IFMT) | format

	m.inodes[path] = inode
	m.markInodeDirty(inode)
	return inode
}

func (m *FileCache) deleteInode(path string) {
	if inode, ok := m.inodes[path]; ok {
		delete(m.inodes, path)
		delete(m.dirtyInodes, inode)
	}

	backend_path := m.getStoragePath(path, ".inode")
	os.Remove(backend_path)
}

func (m *FileCache) OpenFile(path string) (CachedFile, backend.Error) {
	path = normalizePath(path)

	return nil, backend.WrapError(syscall.EIO)
}

func (m *FileCache) putAttr(path string, stat backend.FileStat) {
	inode := m.requireInode(path, stat.Mode()&syscall.S_IFMT)
	updateStatToCache(stat, inode.attr())
	m.markInodeDirty(inode)
}

func (m *FileCache) PutAttr(path string, stat backend.FileStat) {
	path = normalizePath(path)

	m.lock.Lock()
	defer m.lock.Unlock()

	log.Printf("PutAttr(%s, %s)", path, stat)
	m.putAttr(path, stat)
	m.writeback()
}

func (m *FileCache) PutNonExistant(path string) {
	path = normalizePath(path)

	m.lock.Lock()
	defer m.lock.Unlock()

	m.deleteInode(path)
}

func (m *FileCache) fetchAttr(path string) (backend.FileStat, backend.Error) {
	inode, err := m.getInode(path)
	log.Printf("FetchAttr(%s): getInode -> %s, %s", path, inode, err)
	if err != nil {
		return nil, err
	}

	return inode.attr(), nil
}

func (m *FileCache) FetchAttr(path string) (backend.FileStat, backend.Error) {
	path = normalizePath(path)

	m.lock.Lock()
	defer m.lock.Unlock()
	return m.fetchAttr(path)
}

func (m *FileCache) PutLink(path string, dest string) {
	path = normalizePath(path)

	m.lock.Lock()
	defer m.lock.Unlock()

	inode := m.requireInode(path, syscall.S_IFLNK)
	// we don’t need a lock here: the inode was just created and we still
	// hold the lock on the whole cache
	inode.(*linkInode).dest = dest
	m.markInodeDirty(inode)

	m.writeback()
}

func (m *FileCache) FetchLink(path string) (dest string, err backend.Error) {
	path = normalizePath(path)

	m.lock.Lock()
	defer m.lock.Unlock()

	inode, err := m.getInode(path)
	log.Printf("FetchLink(%s): getInode -> %s, %s", path, inode, err)
	if err != nil {
		return "", err
	}

	if inode.attr().Mode()&syscall.S_IFMT != syscall.S_IFLNK {
		log.Printf("FetchLink(%s): not a symlink: %d != %d",
			path,
			inode.attr().Mode()&syscall.S_IFMT,
			syscall.S_IFLNK)
		return "", backend.WrapError(syscall.EINVAL)
	}

	return inode.(*linkInode).dest, nil
}

func (m *FileCache) PutDir(path string, entries []backend.DirEntry) {
	path = normalizePath(path)

	log.Printf("PutDir(%s, %s)", path, entries)

	m.lock.Lock()
	defer m.lock.Unlock()

	inode := m.requireInode(path, syscall.S_IFDIR)
	log.Printf("PutDir(%s): new inode format: %d",
		path,
		inode.attr().Mode()&syscall.S_IFMT)
	dir_inode := inode.(*dirInode)
	dir_inode.children = make([]string, len(entries))
	log.Printf("PutDir(%s): setting up %d children", path, len(entries))
	for i, entry := range entries {
		child_name := entry.Name()
		dir_inode.children[i] = child_name
		child_path := path + "/" + child_name
		m.putAttr(child_path, entry.Stat())
	}
	m.markInodeDirty(inode)

	m.writeback()
}

func (m *FileCache) FetchDir(path string) ([]backend.DirEntry, backend.Error) {
	path = normalizePath(path)

	log.Printf("FetchDir(%s)", path)

	m.lock.Lock()
	defer m.lock.Unlock()

	inode, err := m.getInode(path)
	if err != nil {
		return nil, err
	}

	if inode.attr().Mode()&syscall.S_IFMT != syscall.S_IFDIR {
		log.Printf("FetchDir(%s): not a directory: %d != %d",
			path,
			inode.attr().Mode()&syscall.S_IFMT,
			syscall.S_IFDIR)
		return nil, backend.WrapError(syscall.ENOTDIR)
	}

	dir_inode := inode.(*dirInode)
	result := make([]backend.DirEntry, len(dir_inode.children))
	for i, name := range dir_inode.children {
		full_path := path + "/" + name
		attr, err := m.fetchAttr(full_path)
		if err != nil {
			attr = &dirCacheEntry{}
		}
		result[i] = &dirCacheEntry{
			NameV:   name,
			ModeV:   attr.Mode(),
			MtimeV:  attr.Mtime(),
			AtimeV:  attr.Atime(),
			CtimeV:  attr.Ctime(),
			SizeV:   attr.Size(),
			UidV:    attr.OwnerUID(),
			GidV:    attr.OwnerGID(),
			BlocksV: 0,
		}
	}

	return result, nil
}

func (m *FileCache) Close() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.writeback()
	// TODO: close open file handles
	m.inodes = nil
	m.dirtyInodes = nil
}

func (m *FileCache) SetBlocksTotal(new_blocks uint64) {
	m.quota.blocksTotal = new_blocks
}

func (m *FileCache) BlockSize() int64 {
	return BLOCK_SIZE
}
