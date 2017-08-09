package cache

import "github.com/horazont/dragonstash/internal/backend"

type Interface interface {
	Delete(path string)
	FetchAttr(path string) (backend.FileStat, backend.Error)
	FetchDir(path string) ([]backend.DirEntry, backend.Error)
	FetchLink(path string) (string, backend.Error)
	OpenForStore(path string) (CachedFileHandle, backend.Error)
	PutAttr(path string, stat backend.FileStat)
	PutDir(path string, fs backend.FileSystem, entries []backend.DirEntry)
	PutLink(path string, dest string)
	BlockSize() int64
}

type CachedFileHandle interface {
	PutReadData(data []byte, position int64, at_eof bool)
	ReadData(data []byte, position int64) (int, backend.Error)
	Close()
}
