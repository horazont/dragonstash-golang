package cache

import "github.com/horazont/dragonstash/internal/backend"

type Interface interface {
	PutDir(path string, fs backend.FileSystem, entries []backend.DirEntry)
	FetchDir(path string) ([]backend.DirEntry, backend.Error)
	DelDir(path string)
	FetchAttr(path string) (backend.FileStat, backend.Error)
	PutAttr(path string, stat backend.FileStat)
	PutLink(path string, dest string)
	FetchLink(path string) (string, backend.Error)
}
