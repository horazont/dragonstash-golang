package frontend

import (
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/horazont/dragonstash/internal/layer"
)

type DragonStashFS struct {
	pathfs.FileSystem
	fs layer.FileSystem
}

func NewDragonStashFS(fs layer.FileSystem) *DragonStashFS {
	return &DragonStashFS{
		FileSystem: pathfs.NewDefaultFileSystem(),
		fs:         fs,
	}
}

func (m *DragonStashFS) GetAttr(path string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	stat, err := m.fs.Lstat(path)
	if err != nil {
		return nil, fuse.Status(err.Errno())
	}

	return &fuse.Attr{
		Mode:   stat.Mode(),
		Blocks: stat.Blocks(),
		Mtime:  stat.Mtime(),
		Atime:  stat.Atime(),
		Ctime:  stat.Ctime(),
		Owner:  fuse.Owner{stat.OwnerUID(), stat.OwnerGID()},
		Size:   stat.Size(),
	}, fuse.OK
}

func (m *DragonStashFS) OpenDir(path string, context *fuse.Context) (stream []fuse.DirEntry, code fuse.Status) {
	entries, err := m.fs.OpenDir(path)
	if err != nil {
		return nil, fuse.Status(err.Errno())
	}

	stream = make([]fuse.DirEntry, len(entries))
	for i, entry := range entries {
		stream[i] = fuse.DirEntry{
			Name: entry.Name(),
			Mode: entry.Mode(),
		}
	}
	return stream, fuse.OK
}

func (m *DragonStashFS) Readlink(path string, context *fuse.Context) (string, fuse.Status) {
	result, err := m.fs.Readlink(path)
	if err != nil {
		return "", fuse.Status(err.Errno())
	}

	return result, fuse.OK
}

func (m *DragonStashFS) Open(path string, flags uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	result, err := m.fs.OpenFile(path, int(flags))
	if err != nil {
		return nil, fuse.Status(err.Errno())
	}

	return wrapFile(result), fuse.OK
}

type DragonStashFile struct {
	nodefs.File
	file layer.File
}

func wrapFile(f layer.File) *DragonStashFile {
	return &DragonStashFile{
		nodefs.NewDefaultFile(),
		f,
	}
}

func (m *DragonStashFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	n, err := m.file.Read(dest, off)
	if err != nil {
		return fuse.ReadResultData(dest[:n]), fuse.Status(err.Errno())
	}
	return fuse.ReadResultData(dest[:n]), fuse.OK
}
