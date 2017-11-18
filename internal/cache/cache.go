package cache

import (
	"errors"
	"syscall"
	"time"

	"github.com/horazont/dragonstash/internal/layer"
)

var (
	ErrMustBeAligned = errors.New("This operation must be aligned.")
)

// Notes about put operations:
//
// When a Put operation is executed on a path which is already in a cache but
// the type of the object in the cache and the type of the object created by the
// Put operation differ, the object is replaced atomically nevertheless. In the
// case of directories, this means recursively purging all children from the
// cache.
//
// When changing the type of an object through a Put operation, it also marks
// the attributes of the object as stale (unless the changing operation was a
// PutAttr).
//
// Common error conditions:
//
// ENOENT is returned if the path of a fetch operation is not in the cache, and
// we have evidence that the path does not exist.
//
// EIO is returned if the path of a fetch operation is not in the cache and
// there’s no evidence that it should not exist.
type Cache interface {
	// Open a file
	//
	// Files are always opened in read+write mode.
	//
	// Note: It is recommended to first issue a PutAttr to the file with the
	// most-recent attributes to allow the cache to invalidate itself, if
	// needed.
	OpenFile(path string) (CachedFile, layer.Error)

	// Update a directory with new contents
	//
	// This operation is invasive, in the sense that it will recursively
	// delete entries which are not part of the updated directory structure
	// anymore.
	//
	// It is not necessary that the parent of the path is in the cache.
	//
	// The old and new information is merged.
	PutDir(path string, entries []layer.DirEntry)

	// Update the attributes of a file, symlink or directory
	//
	// If the type of the object changes with this operation, the same as
	// for normal type changes holds, except that the attribute information
	// is considered up-to-date.
	PutAttr(path string, stat layer.FileStat)

	// Put a symlink in the cache
	PutLink(path string, dest string)

	// Mark the path as non-existant.
	//
	// This negative caching is useful in certain situations.
	PutNonExistant(path string)

	// Retrieve a link from the cache
	//
	// Returns EINVAL if the path is something other than a link.
	//
	// The usual error conditions apply.
	FetchLink(path string) (dest string, err layer.Error)

	// Retrieve a directory from the cache
	//
	// Returns ENOTDIR if path is something other than a directory.
	FetchDir(path string) ([]layer.DirEntry, layer.Error)

	// Retrieve the attributes of a path.
	FetchAttr(path string) (layer.FileStat, layer.Error)

	// The block size of the cache
	BlockSize() int64

	// Close all open files and flush dirty buffers to disk
	Close()
}

type CachedFile interface {
	// Write data into the cached file
	//
	// This is used to both write back bytes which have been read as well as
	// write bytes which have been written into the cache.
	//
	// Puts may be rejected if they are not block aligned. A full block
	// write must then be used instead.
	//
	// The indicator whether data was written or read may be used by
	// eviction strategies to decide on whether to evict blocks or not.
	//
	// Returns ErrMustBeAligned if the write must be aligned. No other
	// errors are returned.
	PutData(data []byte, position uint64) error

	// Fetch data from the cache
	//
	// The number of bytes which have been read are returned. Reads to not
	// need to be block aligned, but may be truncated at block boundaries if
	// the next block is not in the cache.
	FetchData(data []byte, position uint64) (int, layer.Error)

	// Return the attributes of the opened file
	//
	// This may differ from the attributes at the opened path iff the file
	// has been renamed or another file was moved over this file.
	FetchAttr() (layer.FileStat, layer.Error)

	// Sync all pending changes to persistent storage
	//
	// The cache must not rely on this to be called. Especially applications
	// which open files in read-only mode won’t call Sync on them.
	Sync()

	Truncate(size uint64) layer.Error
	Chown(uid uint32, gid uint32) layer.Error
	Chmod(perms uint32) layer.Error
	Utimens(atime *time.Time, mtime *time.Time) layer.Error
	Allocate(off uint64, size uint64, mode uint32) layer.Error

	// Close the open file
	Close()
}

type dummyCache struct {
}

func NewDummyCache() Cache {
	return &dummyCache{}
}

func (m *dummyCache) OpenFile(path string) (CachedFile, layer.Error) {
	return nil, layer.WrapError(syscall.EIO)
}

func (m *dummyCache) PutDir(path string, entries []layer.DirEntry) {
}

func (m *dummyCache) PutAttr(path string, attr layer.FileStat) {
}

func (m *dummyCache) PutLink(path string, dest string) {
}

func (m *dummyCache) PutNonExistant(path string) {
}

func (m *dummyCache) FetchLink(path string) (dest string, err layer.Error) {
	return "", layer.WrapError(syscall.EIO)
}

func (m *dummyCache) FetchDir(path string) ([]layer.DirEntry, layer.Error) {
	return nil, layer.WrapError(syscall.EIO)
}

func (m *dummyCache) FetchAttr(path string) (layer.FileStat, layer.Error) {
	return nil, layer.WrapError(syscall.EIO)
}

func (m *dummyCache) BlockSize() int64 {
	return 1
}

func (m *dummyCache) Close() {
}

type dummyCachedFile struct {
}

func NewDummyCachedFile() CachedFile {
	return &dummyCachedFile{}
}

func (m *dummyCachedFile) PutData(data []byte, position uint64) error {
	return nil
}

func (m *dummyCachedFile) FetchData(data []byte, position uint64) (int, layer.Error) {
	return 0, layer.WrapError(syscall.EIO)
}

func (m *dummyCachedFile) FetchAttr() (layer.FileStat, layer.Error) {
	return nil, layer.WrapError(syscall.EIO)
}

func (m *dummyCachedFile) Sync() {
}

func (m *dummyCachedFile) Truncate(size uint64) layer.Error {
	return layer.WrapError(syscall.ENOSYS)
}

func (m *dummyCachedFile) Chown(uid uint32, gid uint32) layer.Error {
	return layer.WrapError(syscall.ENOSYS)
}

func (m *dummyCachedFile) Chmod(perms uint32) layer.Error {
	return layer.WrapError(syscall.ENOSYS)
}

func (m *dummyCachedFile) Utimens(atime *time.Time, mtime *time.Time) layer.Error {
	return layer.WrapError(syscall.ENOSYS)
}

func (m *dummyCachedFile) Allocate(off uint64, size uint64, mode uint32) layer.Error {
	return layer.WrapError(syscall.ENOSYS)
}

func (m *dummyCachedFile) Close() {

}
