package filecache

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/horazont/dragonstash/internal/cache"
	"github.com/horazont/dragonstash/internal/layer"
)

const (
	BLOCK_SIZE = 4096
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

type FileCache struct {
	lock        *sync.Mutex
	root_dir    string
	inodes      map[string]inode
	quota       cache.QuotaInfo
	dirtyInodes map[inode]bool
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

func (m *FileCache) writeback() {
	for inode := range m.dirtyInodes {
		func() {
			inode.Mutex().Lock()
			defer inode.Mutex().Unlock()
			if err := inode.Sync(); err != nil {
				log.Printf("failed to sync inode: %s", err)
			}
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
func (m *FileCache) getInode(path string) (inode, layer.Error) {
	// first try to load the inode from the map
	inode, ok := m.inodes[path]
	if ok {
		return inode, nil
	}

	inode, err := openInode(m.getStoragePath(path, ""))
	if err != nil {
		log.Printf("failed to open inode: %s", err)
		return nil, layer.WrapError(syscall.EIO)
	}
	return inode, nil
}

func (m *FileCache) requireInode(path string, format uint32) inode {
	inode, ok := m.inodes[path]
	if ok && inode.Mode()&syscall.S_IFMT == format {
		// return existing inode if mode matches
		return inode
	} else {
		// TODO: clean up old inode properly
	}

	storage_path := m.getStoragePath(path, "")
	os.MkdirAll(filepath.Dir(storage_path), 0700)
	inode, err := createEmptyInode(storage_path, format)
	if err != nil {
		return nil
	}
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

func (m *FileCache) OpenFile(path string) (cache.CachedFile, layer.Error) {
	path = normalizePath(path)

	return nil, layer.WrapError(syscall.EIO)
}

func (m *FileCache) putAttr(path string, stat layer.FileStat) {
	inode := m.requireInode(path, stat.Mode()&syscall.S_IFMT)
	updateInode(stat, inode)
	m.markInodeDirty(inode)
}

func (m *FileCache) PutAttr(path string, stat layer.FileStat) {
	path = normalizePath(path)

	m.lock.Lock()
	defer m.lock.Unlock()

	log.Printf("PutAttr(%s, %s)", path, stat)
	m.putAttr(path, stat)
}

func (m *FileCache) PutNonExistant(path string) {
	path = normalizePath(path)

	m.lock.Lock()
	defer m.lock.Unlock()

	m.deleteInode(path)
}

func (m *FileCache) fetchAttr(path string) (layer.FileStat, layer.Error) {
	inode, err := m.getInode(path)
	log.Printf("FetchAttr(%s): getInode -> %s, %s", path, inode, err)
	if err != nil {
		return nil, err
	}

	// FIXME: use a copy here
	return inode, nil
}

func (m *FileCache) FetchAttr(path string) (layer.FileStat, layer.Error) {
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

func (m *FileCache) FetchLink(path string) (dest string, err layer.Error) {
	path = normalizePath(path)

	m.lock.Lock()
	defer m.lock.Unlock()

	inode, err := m.getInode(path)
	log.Printf("FetchLink(%s): getInode -> %s, %s", path, inode, err)
	if err != nil {
		return "", err
	}

	if inode.Mode()&syscall.S_IFMT != syscall.S_IFLNK {
		log.Printf("FetchLink(%s): not a symlink: %d != %d",
			path,
			inode.Mode()&syscall.S_IFMT,
			syscall.S_IFLNK)
		return "", layer.WrapError(syscall.EINVAL)
	}

	return inode.(*linkInode).dest, nil
}

func (m *FileCache) PutDir(path string, entries []layer.DirEntry) {
	path = normalizePath(path)

	log.Printf("PutDir(%s, %s)", path, entries)

	m.lock.Lock()
	defer m.lock.Unlock()

	inode := m.requireInode(path, syscall.S_IFDIR)
	log.Printf("PutDir(%s): new inode format: %d",
		path,
		inode.Mode()&syscall.S_IFMT)
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

func (m *FileCache) FetchDir(path string) ([]layer.DirEntry, layer.Error) {
	path = normalizePath(path)

	log.Printf("FetchDir(%s)", path)

	m.lock.Lock()
	defer m.lock.Unlock()

	inode, err := m.getInode(path)
	if err != nil {
		return nil, err
	}

	if inode.Mode()&syscall.S_IFMT != syscall.S_IFDIR {
		log.Printf("FetchDir(%s): not a directory: %d != %d",
			path,
			inode.Mode()&syscall.S_IFMT,
			syscall.S_IFDIR)
		return nil, layer.WrapError(syscall.ENOTDIR)
	}

	dir_inode := inode.(*dirInode)
	result := make([]layer.DirEntry, len(dir_inode.children))
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
	m.quota.BlocksTotal = new_blocks
}

func (m *FileCache) BlockSize() int64 {
	return BLOCK_SIZE
}
