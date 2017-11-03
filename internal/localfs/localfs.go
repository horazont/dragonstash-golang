package localfs

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/horazont/dragonstash/internal/layer"
)

type LocalFileSystem struct {
	root string
}

func NewLocalFileSystem(root string) *LocalFileSystem {
	return &LocalFileSystem{
		root: root,
	}
}

func (m *LocalFileSystem) fullPath(path string) (string, layer.Error) {
	path = filepath.Clean(path)
	return filepath.Join(m.root, path), nil
}

func (m *LocalFileSystem) IsReady() bool {
	return true
}

func (m *LocalFileSystem) Join(elems ...string) string {
	return filepath.Join(elems...)
}

func (m *LocalFileSystem) Lstat(path string) (layer.FileStat, layer.Error) {
	fullPath, fserr := m.fullPath(path)
	if fserr != nil {
		return nil, fserr
	}

	stat, err := os.Lstat(fullPath)
	if err != nil {
		return nil, layer.WrapError(err)
	}

	return wrapFileInfo(stat), nil
}

func (m *LocalFileSystem) OpenDir(path string) ([]layer.DirEntry, layer.Error) {
	path, fserr := m.fullPath(path)
	if fserr != nil {
		return nil, fserr
	}

	dir, err := os.Open(path)
	if err != nil {
		return nil, layer.WrapError(err)
	}
	defer dir.Close()

	entries := []layer.DirEntry{}
	buffer := make([]layer.DirEntry, 2)
	for {
		new, err := dir.Readdir(2)
		if len(new) == 0 {
			if err == io.EOF {
				break
			} else {
				return nil, layer.WrapError(err)
			}
		}

		for i, osentry := range new {
			buffer[i] = wrapFileInfoIntoDirEntry(osentry)
		}
		entries = append(entries, buffer[:len(new)]...)
	}

	return entries, nil
}

func (m *LocalFileSystem) Readlink(path string) (string, layer.Error) {
	path, fserr := m.fullPath(path)
	if fserr != nil {
		return "", fserr
	}

	result, err := os.Readlink(path)
	if err != nil {
		return "", layer.WrapError(err)
	}

	return result, nil
}

func (m *LocalFileSystem) OpenFile(path string, flags int) (layer.File, layer.Error) {
	path, fserr := m.fullPath(path)
	if fserr != nil {
		return nil, fserr
	}

	f, err := os.OpenFile(path, flags, 0)
	if err != nil {
		return nil, layer.WrapError(err)
	}

	return newLocalFile(f), nil
}

type LocalDirEntry struct {
	name    string
	wrapped *LocalFileStat
}

func wrapFileInfoIntoDirEntry(v os.FileInfo) *LocalDirEntry {
	return &LocalDirEntry{
		name:    v.Name(),
		wrapped: wrapFileInfo(v),
	}
}

func (m *LocalDirEntry) Name() string {
	return m.name
}

func (m *LocalDirEntry) Mode() uint32 {
	return m.wrapped.Mode()
}

func (m *LocalDirEntry) Stat() layer.FileStat {
	return m.wrapped
}

type LocalFileStat struct {
	backend syscall.Stat_t
}

func wrapFileInfo(v os.FileInfo) *LocalFileStat {
	return &LocalFileStat{*v.Sys().(*syscall.Stat_t)}
}

func (m *LocalFileStat) Mtime() uint64 {
	return uint64(m.backend.Mtim.Sec)
}

func (m *LocalFileStat) Atime() uint64 {
	return uint64(m.backend.Atim.Sec)
}

func (m *LocalFileStat) Blocks() uint64 {
	return uint64(m.backend.Blocks)
}

func (m *LocalFileStat) Ctime() uint64 {
	return uint64(m.backend.Ctim.Sec)
}

func (m *LocalFileStat) Mode() uint32 {
	return m.backend.Mode
}

func (m *LocalFileStat) OwnerGID() uint32 {
	return m.backend.Gid
}

func (m *LocalFileStat) OwnerUID() uint32 {
	return m.backend.Uid
}

func (m *LocalFileStat) Size() uint64 {
	return uint64(m.backend.Size)
}

type LocalFile struct {
	backend *os.File
	lock    *sync.Mutex
}

func newLocalFile(f *os.File) *LocalFile {
	return &LocalFile{
		backend: f,
		lock:    &sync.Mutex{},
	}
}

func (m *LocalFile) Read(dest []byte, position int64) (int, layer.Error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	n, err := m.backend.ReadAt(dest, position)
	if err == io.EOF {
		err = nil
	}

	if err != nil {
		log.Printf("Read(): %s\n", err)
	}
	return n, layer.WrapError(err)
}

func (m *LocalFile) Stat() (layer.FileStat, layer.Error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	stat, err := m.backend.Stat()
	if err != nil {
		return nil, layer.WrapError(err)
	}

	return wrapFileInfo(stat), nil
}

func (m *LocalFile) Release() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.backend.Close()
}
