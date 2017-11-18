package filecache

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"syscall"
	"testing"

	"github.com/horazont/dragonstash/internal/layer"
	"github.com/stretchr/testify/assert"
)

type mockDirEntry struct {
	NameV   string
	ModeV   uint32
	MtimeV  uint64
	CtimeV  uint64
	AtimeV  uint64
	SizeV   uint64
	UidV    uint32
	GidV    uint32
	BlocksV uint64
}

func (m *mockDirEntry) Mode() uint32 {
	return m.ModeV
}

func (m *mockDirEntry) Atime() uint64 {
	return m.AtimeV
}

func (m *mockDirEntry) Blocks() uint64 {
	return m.BlocksV
}

func (m *mockDirEntry) Ctime() uint64 {
	return m.CtimeV
}

func (m *mockDirEntry) Mtime() uint64 {
	return m.MtimeV
}

func (m *mockDirEntry) OwnerGID() uint32 {
	return m.GidV
}

func (m *mockDirEntry) OwnerUID() uint32 {
	return m.UidV
}

func (m *mockDirEntry) Size() uint64 {
	return m.SizeV
}

func (m *mockDirEntry) Stat() layer.FileStat {
	return m
}

func (m *mockDirEntry) Name() string {
	return m.NameV
}

func prepTempDir() string {
	path, err := ioutil.TempDir("", "dragonstash-test")
	if err != nil {
		panic(fmt.Sprintf("Error: %s", err))
	}
	log.Printf("using temporary directory: %s", path)
	return path
}

func teardownTempDir(path string) {
	os.RemoveAll(path)
}

func TestPutAndFetchAttr(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	cache := NewFileCache(dir)
	attr1 := mockDirEntry{
		ModeV:   syscall.S_IFDIR,
		MtimeV:  1234,
		AtimeV:  2345,
		CtimeV:  3456,
		SizeV:   4567,
		UidV:    6789,
		GidV:    7890,
		BlocksV: 1024,
	}

	cache.PutAttr("/some/arbitrary/path", &attr1)
	attr2, err := cache.FetchAttr("/some/arbitrary/path")

	assert.Nil(t, err)
	assert.NotNil(t, attr2)
	assert.Equal(t, attr1.ModeV, attr2.Mode())
	assert.Equal(t, attr1.MtimeV, attr2.Mtime())
	assert.Equal(t, attr1.AtimeV, attr2.Atime())
	assert.Equal(t, attr1.CtimeV, attr2.Ctime())
	assert.Equal(t, attr1.SizeV, attr2.Size())
	assert.Equal(t, attr1.UidV, attr2.OwnerUID())
	assert.Equal(t, attr1.GidV, attr2.OwnerGID())
	assert.Equal(t, uint64(0), attr2.Blocks())

	cache.Close()
}

func TestPutAndFetchAttrPersistency(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	cache_w := NewFileCache(dir)
	attr1 := mockDirEntry{
		ModeV:   syscall.S_IFDIR,
		MtimeV:  1234,
		AtimeV:  2345,
		CtimeV:  3456,
		SizeV:   4567,
		UidV:    6789,
		GidV:    7890,
		BlocksV: 1024,
	}

	cache_w.PutAttr("/some/arbitrary/path", &attr1)
	cache_w.Close()

	cache_r := NewFileCache(dir)
	attr2, err := cache_r.FetchAttr("/some/arbitrary/path")

	assert.Nil(t, err)
	assert.NotNil(t, attr2)
	assert.Equal(t, attr1.ModeV, attr2.Mode())
	assert.Equal(t, attr1.MtimeV, attr2.Mtime())
	assert.Equal(t, attr1.AtimeV, attr2.Atime())
	assert.Equal(t, attr1.CtimeV, attr2.Ctime())
	assert.Equal(t, attr1.SizeV, attr2.Size())
	assert.Equal(t, attr1.UidV, attr2.OwnerUID())
	assert.Equal(t, attr1.GidV, attr2.OwnerGID())
	assert.Equal(t, uint64(0), attr2.Blocks())
	cache_r.Close()
}

func TestPutNonExistantRemovesAttr(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	cache := NewFileCache(dir)
	attr1 := mockDirEntry{
		ModeV:   syscall.S_IFDIR,
		MtimeV:  1234,
		AtimeV:  2345,
		CtimeV:  3456,
		SizeV:   4567,
		UidV:    6789,
		GidV:    7890,
		BlocksV: 1024,
	}

	cache.PutAttr("/some/arbitrary/path", &attr1)
	cache.PutNonExistant("/some/arbitrary/path")
	attr2, err := cache.FetchAttr("/some/arbitrary/path")

	assert.Nil(t, attr2)
	assert.NotNil(t, err)

	// TODO: assert that ENOENT is given instead of EIO
	// we don’t implement that currently.

	cache.Close()
}

func TestPutLinkBeforePutAttr(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	cache := NewFileCache(dir)

	cache.PutLink("/some/arbitrary/path", "../other/path")
	dest, err := cache.FetchLink("/some/arbitrary/path")

	assert.Nil(t, err)
	assert.Equal(t, dest, "../other/path")

	cache.Close()
}

func TestPutLinkAfterPutAttrPreservesAttributes(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	cache := NewFileCache(dir)

	attr1 := mockDirEntry{
		ModeV:   syscall.S_IFLNK,
		MtimeV:  1234,
		AtimeV:  2345,
		CtimeV:  3456,
		SizeV:   4567,
		UidV:    6789,
		GidV:    7890,
		BlocksV: 1024,
	}

	cache.PutAttr("/some/arbitrary/path", &attr1)
	cache.PutLink("/some/arbitrary/path", "../other/path")
	attr2, err := cache.FetchAttr("/some/arbitrary/path")

	assert.Nil(t, err)
	assert.NotNil(t, attr2)

	assert.Equal(t, attr1.ModeV, attr2.Mode())
	assert.Equal(t, attr1.MtimeV, attr2.Mtime())
	assert.Equal(t, attr1.AtimeV, attr2.Atime())
	assert.Equal(t, attr1.CtimeV, attr2.Ctime())
	assert.Equal(t, attr1.SizeV, attr2.Size())
	assert.Equal(t, attr1.UidV, attr2.OwnerUID())
	assert.Equal(t, attr1.GidV, attr2.OwnerGID())
	assert.Equal(t, uint64(0), attr2.Blocks())

	cache.Close()
}

func TestPutLinkPersistence(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	cache_w := NewFileCache(dir)
	cache_w.PutLink("/some/arbitrary/path", "../other/path")
	cache_w.Close()

	cache_r := NewFileCache(dir)

	dest, err := cache_r.FetchLink("/some/arbitrary/path")

	assert.Nil(t, err)
	assert.Equal(t, dest, "../other/path")

	cache_r.Close()
}

func TestPutDirAndFetchDir(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	cache := NewFileCache(dir)

	entries := make([]layer.DirEntry, 3)
	entries[0] = &mockDirEntry{
		NameV:   "foo",
		ModeV:   syscall.S_IFREG,
		MtimeV:  11,
		AtimeV:  12,
		CtimeV:  13,
		SizeV:   1023,
		UidV:    0,
		GidV:    0,
		BlocksV: 1,
	}
	entries[1] = &mockDirEntry{
		NameV:   "bar",
		ModeV:   syscall.S_IFREG,
		MtimeV:  21,
		AtimeV:  22,
		CtimeV:  23,
		SizeV:   3023,
		UidV:    1000,
		GidV:    1000,
		BlocksV: 2,
	}
	entries[2] = &mockDirEntry{
		NameV:   "baz",
		ModeV:   syscall.S_IFREG,
		MtimeV:  31,
		AtimeV:  32,
		CtimeV:  33,
		SizeV:   10023,
		UidV:    0,
		GidV:    0,
		BlocksV: 4,
	}

	entrymap := make(map[string]layer.DirEntry)
	for _, entry := range entries {
		entrymap[entry.Name()] = entry
	}

	cache.PutDir("/some/dir", entries)

	entries2, err := cache.FetchDir("/some/dir")

	assert.NotNil(t, entries2)
	assert.Nil(t, err)

	entrymap2 := make(map[string]layer.DirEntry)
	for _, entry := range entries2 {
		entrymap2[entry.Name()] = entry
	}

	assert.Equal(t, len(entries), len(entries2))

	for key, entry1 := range entrymap {
		entry2, ok := entrymap2[key]
		assert.True(t, ok)

		assert.Equal(t, entry1.Mode(), entry2.Mode())
		assert.Equal(t, entry1.Stat().Mtime(), entry2.Stat().Mtime())
		assert.Equal(t, entry1.Stat().Atime(), entry2.Stat().Atime())
		assert.Equal(t, entry1.Stat().Ctime(), entry2.Stat().Ctime())
		assert.Equal(t, entry1.Stat().Size(), entry2.Stat().Size())
		assert.Equal(t, entry1.Stat().OwnerUID(), entry2.Stat().OwnerUID())
		assert.Equal(t, entry1.Stat().OwnerGID(), entry2.Stat().OwnerGID())
		assert.Equal(t, uint64(0), entry2.Stat().Blocks())
	}
}

func TestPutDirAndFetchAttr(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	cache := NewFileCache(dir)

	entries := make([]layer.DirEntry, 3)
	entries[0] = &mockDirEntry{
		NameV:   "foo",
		ModeV:   syscall.S_IFREG,
		MtimeV:  11,
		AtimeV:  12,
		CtimeV:  13,
		SizeV:   1023,
		UidV:    0,
		GidV:    0,
		BlocksV: 1,
	}
	entries[1] = &mockDirEntry{
		NameV:   "bar",
		ModeV:   syscall.S_IFREG,
		MtimeV:  21,
		AtimeV:  22,
		CtimeV:  23,
		SizeV:   3023,
		UidV:    1000,
		GidV:    1000,
		BlocksV: 2,
	}
	entries[2] = &mockDirEntry{
		NameV:   "baz",
		ModeV:   syscall.S_IFREG,
		MtimeV:  31,
		AtimeV:  32,
		CtimeV:  33,
		SizeV:   10023,
		UidV:    0,
		GidV:    0,
		BlocksV: 4,
	}

	cache.PutDir("/some/dir", entries)

	for _, entry := range entries {
		attr2, err := cache.FetchAttr("/some/dir/" + entry.Name())
		assert.Nil(t, err)
		assert.NotNil(t, attr2)

		assert.Equal(t, entry.Mode(), attr2.Mode())
		assert.Equal(t, entry.Stat().Mtime(), attr2.Mtime())
		assert.Equal(t, entry.Stat().Atime(), attr2.Atime())
		assert.Equal(t, entry.Stat().Ctime(), attr2.Ctime())
		assert.Equal(t, entry.Stat().Size(), attr2.Size())
		assert.Equal(t, entry.Stat().OwnerUID(), attr2.OwnerUID())
		assert.Equal(t, entry.Stat().OwnerGID(), attr2.OwnerGID())
		assert.Equal(t, uint64(0), attr2.Blocks())
	}
}

func TestPutDirPersistence(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	cache_w := NewFileCache(dir)

	entries := make([]layer.DirEntry, 3)
	entries[0] = &mockDirEntry{
		NameV:   "foo",
		ModeV:   syscall.S_IFREG,
		MtimeV:  11,
		AtimeV:  12,
		CtimeV:  13,
		SizeV:   1023,
		UidV:    0,
		GidV:    0,
		BlocksV: 1,
	}
	entries[1] = &mockDirEntry{
		NameV:   "bar",
		ModeV:   syscall.S_IFREG,
		MtimeV:  21,
		AtimeV:  22,
		CtimeV:  23,
		SizeV:   3023,
		UidV:    1000,
		GidV:    1000,
		BlocksV: 2,
	}
	entries[2] = &mockDirEntry{
		NameV:   "baz",
		ModeV:   syscall.S_IFREG,
		MtimeV:  31,
		AtimeV:  32,
		CtimeV:  33,
		SizeV:   10023,
		UidV:    0,
		GidV:    0,
		BlocksV: 4,
	}

	entrymap := make(map[string]layer.DirEntry)
	for _, entry := range entries {
		entrymap[entry.Name()] = entry
	}

	cache_w.PutDir("/some/dir", entries)
	cache_w.Close()

	cache_r := NewFileCache(dir)

	entries2, err := cache_r.FetchDir("/some/dir")

	assert.NotNil(t, entries2)
	assert.Nil(t, err)

	entrymap2 := make(map[string]layer.DirEntry)
	for _, entry := range entries2 {
		entrymap2[entry.Name()] = entry
	}

	assert.Equal(t, len(entries), len(entries2))

	for key, entry1 := range entrymap {
		entry2, ok := entrymap2[key]
		assert.True(t, ok)

		assert.Equal(t, entry1.Mode(), entry2.Mode())
		assert.Equal(t, entry1.Stat().Mtime(), entry2.Stat().Mtime())
		assert.Equal(t, entry1.Stat().Atime(), entry2.Stat().Atime())
		assert.Equal(t, entry1.Stat().Ctime(), entry2.Stat().Ctime())
		assert.Equal(t, entry1.Stat().Size(), entry2.Stat().Size())
		assert.Equal(t, entry1.Stat().OwnerUID(), entry2.Stat().OwnerUID())
		assert.Equal(t, entry1.Stat().OwnerGID(), entry2.Stat().OwnerGID())
		assert.Equal(t, uint64(0), entry2.Stat().Blocks())
	}

	cache_r.Close()
}

func TestEmptyStringAndSlashAreEquivalentForFetchAttr(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	cache := NewFileCache(dir)
	attr1 := mockDirEntry{
		ModeV:   syscall.S_IFDIR,
		MtimeV:  1234,
		AtimeV:  2345,
		CtimeV:  3456,
		SizeV:   4567,
		UidV:    6789,
		GidV:    7890,
		BlocksV: 1024,
	}

	cache.PutAttr("", &attr1)
	attr2, err := cache.FetchAttr("/")

	assert.Nil(t, err)
	assert.NotNil(t, attr2)

	attr2, err = cache.FetchAttr("")

	assert.Nil(t, err)
	assert.NotNil(t, attr2)

	cache.Close()
}

func TestOpenFile(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	cache := NewFileCache(dir)

	attr1 := mockDirEntry{
		ModeV: syscall.S_IFREG,
	}

	cache.PutAttr("/foo", &attr1)

	f, err := cache.OpenFile("/foo")
	assert.Nil(t, err)
	assert.NotNil(t, f)

	cache.Close()
}

func TestOpenFilePutDataPersistency(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)
	var err error
	size := uint64(4096 + 2048)

	cache := NewFileCache(dir)

	attr1 := mockDirEntry{
		ModeV: syscall.S_IFREG,
	}

	cache.PutAttr("/foo", &attr1)

	f, err := cache.OpenFile("/foo")
	assert.Nil(t, err)
	assert.NotNil(t, f)

	ref := genData(int(size))

	err = f.PutData(ref, 0)
	assert.Nil(t, err)

	cache.Close()

	cache_r := NewFileCache(dir)

	f, err = cache_r.OpenFile("/foo")
	assert.Nil(t, err)
	assert.NotNil(t, f)

	attr2, err := cache_r.FetchAttr("/foo")
	assert.Nil(t, err)
	assert.Equal(t, size, attr2.Size())
	assert.Equal(t, uint64(2), attr2.Blocks())

	buf := make([]byte, size+1)
	n, err := f.FetchData(buf, 0)
	assert.Equal(t, int(size), n)
	assert.Equal(t, ref, buf[:size])
}

func TestOpenFileIdempotent(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	cache := NewFileCache(dir)

	attr1 := mockDirEntry{
		ModeV: syscall.S_IFREG,
	}

	cache.PutAttr("/foo", &attr1)

	f1, err := cache.OpenFile("/foo")
	assert.Nil(t, err)
	assert.NotNil(t, f1)

	f2, err := cache.OpenFile("/foo")
	assert.Nil(t, err)
	assert.NotNil(t, f2)

	assert.Equal(t, f1, f2)

	cache.Close()
}

func TestOpenFileIdempotentWithClose(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	cache := NewFileCache(dir)

	attr1 := mockDirEntry{
		ModeV: syscall.S_IFREG,
	}

	cache.PutAttr("/foo", &attr1)

	f1, err := cache.OpenFile("/foo")
	assert.Nil(t, err)
	assert.NotNil(t, f1)

	f2, err := cache.OpenFile("/foo")
	assert.Nil(t, err)
	assert.NotNil(t, f2)

	assert.Equal(t, f1, f2)

	f2.Close()

	f3, err := cache.OpenFile("/foo")
	assert.Nil(t, err)
	assert.NotNil(t, f3)

	assert.Equal(t, f1, f3)

	cache.Close()
}
