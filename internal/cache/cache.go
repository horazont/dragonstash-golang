package cache

import (
	"errors"
	"syscall"

	"github.com/horazont/dragonstash/internal/backend"
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
	OpenFile(path string) (CachedFile, backend.Error)

	// Update a directory with new contents
	//
	// This operation is invasive, in the sense that it will recursively
	// delete entries which are not part of the updated directory structure
	// anymore.
	//
	// It is not necessary that the parent of the path is in the cache.
	//
	// The old and new information is merged.
	PutDir(path string, entries []backend.DirEntry)

	// Update the attributes of a file, symlink or directory
	//
	// If the type of the object changes with this operation, the same as
	// for normal type changes holds, except that the attribute information
	// is considered up-to-date.
	PutAttr(path string, stat backend.FileStat)

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
	FetchLink(path string) (dest string, err backend.Error)

	// Retrieve a directory from the cache
	//
	// Returns ENOTDIR if path is something other than a directory.
	FetchDir(path string) ([]backend.DirEntry, backend.Error)

	// Retrieve the attributes of a path.
	FetchAttr(path string) (backend.FileStat, backend.Error)

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
	PutData(data []byte, position int64, at_eof bool, written bool) backend.Error

	// Fetch data from the cache
	//
	// The number of bytes which have been read are returned. Reads to not
	// need to be block aligned, but may be truncated at block boundaries if
	// the next block is not in the cache.
	FetchData(data []byte, position int64) (int, backend.Error)

	// Close the open file
	Close()
}

type dummyCache struct {
}

func NewDummyCache() Cache {
	return &dummyCache{}
}

func (m *dummyCache) OpenFile(path string) (CachedFile, backend.Error) {
	return nil, backend.WrapError(syscall.EIO)
}

func (m *dummyCache) PutDir(path string, entries []backend.DirEntry) {
}

func (m *dummyCache) PutAttr(path string, attr backend.FileStat) {
}

func (m *dummyCache) PutLink(path string, dest string) {
}

func (m *dummyCache) PutNonExistant(path string) {
}

func (m *dummyCache) FetchLink(path string) (dest string, err backend.Error) {
	return "", backend.WrapError(syscall.EIO)
}

func (m *dummyCache) FetchDir(path string) ([]backend.DirEntry, backend.Error) {
	return nil, backend.WrapError(syscall.EIO)
}

func (m *dummyCache) FetchAttr(path string) (backend.FileStat, backend.Error) {
	return nil, backend.WrapError(syscall.EIO)
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

func (m *dummyCachedFile) PutData(data []byte, position int64, at_eof bool,
	written bool) backend.Error {
	return nil
}

func (m *dummyCachedFile) FetchData(data []byte, position int64) (int, backend.Error) {
	return 0, backend.WrapError(syscall.EIO)
}

func (m *dummyCachedFile) Close() {

}
