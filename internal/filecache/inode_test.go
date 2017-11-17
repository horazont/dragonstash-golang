package filecache

import (
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateInode_Link(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)
	path := dir + "/file"

	ref := dirCacheEntry{
		ModeV:   syscall.S_IFLNK | syscall.S_IRWXU | syscall.S_IRWXG | syscall.S_IRWXO,
		MtimeV:  12,
		AtimeV:  23,
		CtimeV:  34,
		SizeV:   45,
		BlocksV: 56,
		UidV:    78,
		GidV:    89,
	}

	n, err := createInode(path, &ref)
	assert.Nil(t, err)
	assert.NotNil(t, n)

	li, ok := n.(*linkInode)
	assert.NotNil(t, li)
	assert.True(t, ok)

	assert.Equal(t, ref.ModeV, n.Mode())
	assert.Equal(t, ref.MtimeV, n.Mtime())
	assert.Equal(t, ref.CtimeV, n.Ctime())
	assert.Equal(t, ref.AtimeV, n.Atime())
	assert.Equal(t, ref.SizeV, n.Size())
	assert.Equal(t, ref.UidV, n.OwnerUID())
	assert.Equal(t, ref.GidV, n.OwnerGID())
	assert.Equal(t, uint64(0), n.Blocks())

	assert.Equal(t, "", li.dest)
}

func TestCreateInode_Directory(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)
	path := dir + "/file"

	ref := dirCacheEntry{
		ModeV:   syscall.S_IFDIR | syscall.S_IRUSR | syscall.S_IWUSR | syscall.S_IXUSR,
		MtimeV:  12,
		AtimeV:  23,
		CtimeV:  34,
		SizeV:   45,
		BlocksV: 56,
		UidV:    78,
		GidV:    89,
	}

	n, err := createInode(path, &ref)
	assert.Nil(t, err)
	assert.NotNil(t, n)

	di, ok := n.(*dirInode)
	assert.NotNil(t, di)
	assert.True(t, ok)

	assert.Equal(t, ref.ModeV, n.Mode())
	assert.Equal(t, ref.MtimeV, n.Mtime())
	assert.Equal(t, ref.CtimeV, n.Ctime())
	assert.Equal(t, ref.AtimeV, n.Atime())
	assert.Equal(t, ref.SizeV, n.Size())
	assert.Equal(t, ref.UidV, n.OwnerUID())
	assert.Equal(t, ref.GidV, n.OwnerGID())
	assert.Equal(t, uint64(0), n.Blocks())

	assert.Equal(t, 0, len(di.children))
}

func TestCreateInode_Regular(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)
	path := dir + "/file"

	ref := dirCacheEntry{
		ModeV:   syscall.S_IFREG | syscall.S_IRUSR | syscall.S_IWUSR | syscall.S_IRGRP,
		MtimeV:  12,
		AtimeV:  23,
		CtimeV:  34,
		SizeV:   45,
		BlocksV: 56,
		UidV:    78,
		GidV:    89,
	}

	n, err := createInode(path, &ref)
	assert.Nil(t, err)
	assert.NotNil(t, n)

	fi, ok := n.(*fileInode)
	assert.NotNil(t, fi)
	assert.True(t, ok)

	assert.Equal(t, ref.ModeV, n.Mode())
	assert.Equal(t, ref.MtimeV, n.Mtime())
	assert.Equal(t, ref.CtimeV, n.Ctime())
	assert.Equal(t, ref.AtimeV, n.Atime())
	assert.Equal(t, ref.SizeV, n.Size())
	assert.Equal(t, ref.UidV, n.OwnerUID())
	assert.Equal(t, ref.GidV, n.OwnerGID())
	assert.Equal(t, uint64(0), n.Blocks())
}

func TestCreateAndReopenLinkInode(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)
	path := dir + "/file"

	ref := dirCacheEntry{
		ModeV:   syscall.S_IFLNK | syscall.S_IRWXU | syscall.S_IRWXG | syscall.S_IRWXO,
		MtimeV:  12,
		AtimeV:  23,
		CtimeV:  34,
		SizeV:   45,
		BlocksV: 56,
		UidV:    78,
		GidV:    89,
	}

	n, err := createInode(path, &ref)

	assert.Nil(t, err)
	assert.NotNil(t, n)

	li, ok := n.(*linkInode)
	assert.NotNil(t, li)
	assert.True(t, ok)

	li.SetDest("/some/path")
	assert.Nil(t, li.Sync())

	n, err = openInode(path)
	assert.Nil(t, err)
	assert.NotNil(t, n)

	assert.Equal(t, path, n.(*linkInode).storage_path)

	li, ok = n.(*linkInode)
	assert.NotNil(t, li)
	assert.True(t, ok)

	assert.Equal(t, ref.ModeV, n.Mode())
	assert.Equal(t, ref.MtimeV, n.Mtime())
	assert.Equal(t, ref.CtimeV, n.Ctime())
	assert.Equal(t, ref.AtimeV, n.Atime())
	assert.Equal(t, ref.SizeV, n.Size())
	assert.Equal(t, ref.UidV, n.OwnerUID())
	assert.Equal(t, ref.GidV, n.OwnerGID())
	assert.Equal(t, uint64(0), n.Blocks())

	assert.Equal(t, "/some/path", li.dest)
}

func TestCreateAndReopenDirInode(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)
	path := dir + "/file"

	ref := dirCacheEntry{
		ModeV:   syscall.S_IFDIR | syscall.S_IRWXU | syscall.S_IRWXG | syscall.S_IRWXO,
		MtimeV:  12,
		AtimeV:  23,
		CtimeV:  34,
		SizeV:   45,
		BlocksV: 56,
		UidV:    78,
		GidV:    89,
	}

	n, err := createInode(path, &ref)

	assert.Nil(t, err)
	assert.NotNil(t, n)

	di, ok := n.(*dirInode)
	assert.NotNil(t, di)
	assert.True(t, ok)

	di.children = append(di.children, "foo")
	di.children = append(di.children, "fnord")
	di.children = append(di.children, "quux")
	assert.Nil(t, di.Sync())

	n, err = openInode(path)
	assert.Nil(t, err)
	assert.NotNil(t, n)

	di, ok = n.(*dirInode)
	assert.NotNil(t, di)
	assert.True(t, ok)

	assert.Equal(t, ref.ModeV, n.Mode())
	assert.Equal(t, ref.MtimeV, n.Mtime())
	assert.Equal(t, ref.CtimeV, n.Ctime())
	assert.Equal(t, ref.AtimeV, n.Atime())
	assert.Equal(t, ref.SizeV, n.Size())
	assert.Equal(t, ref.UidV, n.OwnerUID())
	assert.Equal(t, ref.GidV, n.OwnerGID())
	assert.Equal(t, uint64(0), n.Blocks())

	assert.Equal(t, 3, len(di.children))
	assert.Equal(t, "foo", di.children[0])
	assert.Equal(t, "fnord", di.children[1])
	assert.Equal(t, "quux", di.children[2])
}
