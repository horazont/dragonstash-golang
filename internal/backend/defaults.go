package backend

import (
	"path"
	"syscall"
)

type DefaultFileStat struct {
}

func NewDefaultFileStat() *DefaultFileStat {
	return &DefaultFileStat{}
}

func (m *DefaultFileStat) Atime() uint64 {
	return 0
}

func (m *DefaultFileStat) Mtime() uint64 {
	return 0
}

func (m *DefaultFileStat) Ctime() uint64 {
	return 0
}

func (m *DefaultFileStat) Size() uint64 {
	return 0
}

func (m *DefaultFileStat) Blocks() uint64 {
	return 0
}

func (m *DefaultFileStat) OwnerUID() uint32 {
	return 0
}

func (m *DefaultFileStat) OwnerGID() uint32 {
	return 0
}

func (m *DefaultFileStat) Mode() uint32 {
	return 0
}

type DefaultDirEntry struct {
}

func NewDefaultDirEntry() *DefaultDirEntry {
	return &DefaultDirEntry{}
}

func (m *DefaultDirEntry) Stat() FileStat {
	return nil
}

func (m *DefaultDirEntry) Mode() uint32 {
	return m.Stat().Mode()
}

type DefaultFileSystem struct {
}

func NewDefaultFileSystem() *DefaultFileSystem {
	return &DefaultFileSystem{}
}

func (m *DefaultFileSystem) IsReady() bool {
	return false
}

func (m *DefaultFileSystem) Join(elems ...string) string {
	return path.Join(elems...)
}

func (m *DefaultFileSystem) Lstat(path string) (FileStat, Error) {
	return nil, NewBackendError("not implemented", syscall.ENOSYS)
}

func (m *DefaultFileSystem) OpenDir(path string) ([]DirEntry, Error) {
	return nil, NewBackendError("not implemented", syscall.ENOSYS)
}

func (m *DefaultFileSystem) Readlink(path string) (string, Error) {
	return "", NewBackendError("not implemented", syscall.ENOSYS)
}

func (m *DefaultFileSystem) OpenFile(path string, flags int) (File, Error) {
	return nil, NewBackendError("not implemented", syscall.ENOSYS)
}
