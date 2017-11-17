package filecache

import (
	"crypto/rand"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockQuotaService struct {
	mock.Mock
}

func (m *mockQuotaService) RequestBlocks(nblocks uint64, priority int) (granted uint64) {
	return uint64(m.Called(nblocks, priority).Int(0))
}

func (m *mockQuotaService) ReleaseBlocks(nblocks uint64) {
	m.Called(nblocks)
}

func genData(nbytes int) (result []byte) {
	result = make([]byte, nbytes)
	rand.Read(result)
	return result
}

func TestAlignedPutAndFetch(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	var err error

	quota := &mockQuotaService{}
	inode, err := createEmptyInode(dir+"/file", syscall.S_IFREG)

	f, err := openFileCachedFile(quota, inode.(*fileInode))
	assert.Nil(t, err)
	assert.NotNil(t, f)

	data := genData(4096)

	err = f.PutData(data, 8192)
	assert.Nil(t, err)

	ref := make([]byte, len(data))
	n, err := f.FetchData(ref, 8192)

	assert.Nil(t, err)
	assert.Equal(t, 4096, n)
	assert.Equal(t, data, ref)
}

func TestFetchOutsideWrittenRange(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	var err error

	quota := &mockQuotaService{}
	inode, err := createEmptyInode(dir+"/file", syscall.S_IFREG)

	f, err := openFileCachedFile(quota, inode.(*fileInode))
	assert.Nil(t, err)
	assert.NotNil(t, f)

	data := genData(4096)

	err = f.PutData(data, 8192)
	assert.Nil(t, err)

	ref := make([]byte, len(data))
	n, err := f.FetchData(ref, 0)

	assert.NotNil(t, err)
	assert.Equal(t, 0, n)
}

func TestAlignedPutAndUnalignedRead(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	var err error

	quota := &mockQuotaService{}
	inode, err := createEmptyInode(dir+"/file", syscall.S_IFREG)

	f, err := openFileCachedFile(quota, inode.(*fileInode))
	assert.Nil(t, err)
	assert.NotNil(t, f)

	data := genData(4096)

	err = f.PutData(data, 8192)
	assert.Nil(t, err)

	ref := make([]byte, len(data)-23)
	n, err := f.FetchData(ref, 8192+23)

	assert.Nil(t, err)
	assert.Equal(t, 4096-23, n)
	assert.Equal(t, data[23:], ref)
}

func TestAppendCanWriteWithoutAlignment(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	var err error

	quota := &mockQuotaService{}
	inode, err := createEmptyInode(dir+"/file", syscall.S_IFREG)

	f, err := openFileCachedFile(quota, inode.(*fileInode))
	assert.Nil(t, err)
	assert.NotNil(t, f)

	data_pad := genData(4096 + 1024)
	data_append := genData(4096)

	err = f.PutData(data_pad, 0)
	assert.Nil(t, err)

	err = f.PutData(data_append[:1024], 4096+1024)
	assert.Nil(t, err)

	err = f.PutData(data_append[1024:3072], 4096+2048)
	assert.Nil(t, err)

	ref := make([]byte, 8192)
	n, err := f.FetchData(ref, 0)

	assert.Nil(t, err)
	assert.Equal(t, 8192, n)
	assert.Equal(t, data_pad, ref[:5120])
	assert.Equal(t, data_append[:3072], ref[5120:])
}

func TestFetchAttrUsesInode(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	ref := dirCacheEntry{
		ModeV:   syscall.S_IFREG | syscall.S_IRWXU,
		MtimeV:  12,
		CtimeV:  23,
		AtimeV:  34,
		SizeV:   103,
		UidV:    10,
		GidV:    20,
		BlocksV: 1,
	}

	var err error

	quota := &mockQuotaService{}
	inode, err := createInode(dir+"/file", &ref)

	f, err := openFileCachedFile(quota, inode.(*fileInode))
	assert.Nil(t, err)
	assert.NotNil(t, f)

	attr, err := f.FetchAttr()
	assert.Nil(t, err)
	assert.NotNil(t, attr)

	assert.Equal(t, ref.ModeV, attr.Mode())
	assert.Equal(t, ref.MtimeV, attr.Mtime())
	assert.Equal(t, ref.AtimeV, attr.Atime())
	assert.Equal(t, ref.CtimeV, attr.Ctime())
	assert.Equal(t, ref.SizeV, attr.Size())
	assert.Equal(t, ref.UidV, attr.OwnerUID())
	assert.Equal(t, ref.GidV, attr.OwnerGID())
}

func TestFetchAttrReturnsProperBlockCount(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	ref := dirCacheEntry{
		ModeV:   syscall.S_IFREG | syscall.S_IRWXU,
		MtimeV:  12,
		CtimeV:  23,
		AtimeV:  34,
		SizeV:   103,
		UidV:    10,
		GidV:    20,
		BlocksV: 1,
	}

	var err error

	quota := &mockQuotaService{}
	inode, err := createInode(dir+"/file", &ref)

	f, err := openFileCachedFile(quota, inode.(*fileInode))
	assert.Nil(t, err)
	assert.NotNil(t, f)

	attr, err := f.FetchAttr()
	assert.Nil(t, err)
	assert.NotNil(t, attr)

	assert.Equal(t, uint64(0), attr.Blocks())
	assert.Equal(t, uint64(1), ref.BlocksV)

	data := genData(4096)

	err = f.PutData(data, 8192)
	assert.Nil(t, err)

	err = f.PutData(data, 0)
	assert.Nil(t, err)

	attr, err = f.FetchAttr()
	assert.Nil(t, err)
	assert.NotNil(t, attr)

	assert.Equal(t, uint64(2), attr.Blocks())
}

func TestChownModifiesInode(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	ref := dirCacheEntry{
		ModeV:   syscall.S_IFREG | syscall.S_IRWXU,
		MtimeV:  12,
		CtimeV:  23,
		AtimeV:  34,
		SizeV:   103,
		UidV:    10,
		GidV:    20,
		BlocksV: 1,
	}

	var err error

	quota := &mockQuotaService{}
	inode, err := createInode(dir+"/file", &ref)

	f, err := openFileCachedFile(quota, inode.(*fileInode))
	assert.Nil(t, err)
	assert.NotNil(t, f)

	err = f.Chown(123, 456)
	assert.Nil(t, err)

	assert.Equal(t, uint32(123), inode.OwnerUID())
	assert.Equal(t, uint32(456), inode.OwnerGID())
}
