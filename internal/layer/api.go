package layer

type FileSystem interface {
	Lstat(path string) (FileStat, Error)
	OpenDir(path string) ([]DirEntry, Error)
	OpenFile(path string, flags int) (File, Error)
	Readlink(path string) (string, Error)
	Join(elems ...string) string
	IsReady() bool
}

type File interface {
	Read(dest []byte, position int64) (int, Error)
	Release()
}

type DirEntry interface {
	Name() string
	Mode() uint32
	Stat() FileStat
}

type FileStat interface {
	Mtime() uint64
	Atime() uint64
	Ctime() uint64
	Size() uint64
	Blocks() uint64
	OwnerUID() uint32
	OwnerGID() uint32
	Mode() uint32
}

type Error interface {
	error
	Errno() uintptr
}
