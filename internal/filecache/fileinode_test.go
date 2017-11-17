package filecache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAutocreatesEmptyFile(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	file := dir + "/file"

	bm, err := openOrCreateFileInode(file)
	assert.Nil(t, err)
	assert.NotNil(t, bm)

	assert.Equal(t, uint64(0), bm.Size())
}

func TestResize(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	file := dir + "/file"

	bm, err := openOrCreateFileInode(file)
	assert.Nil(t, err)
	assert.NotNil(t, bm)

	bm.Resize(1024)

	assert.Equal(t, uint64(1024), bm.Size())
}

func TestResizePersistence(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	file := dir + "/file"

	bm, err := openOrCreateFileInode(file)
	assert.Nil(t, err)
	assert.NotNil(t, bm)

	bm.Resize(1024)

	assert.Equal(t, uint64(1024), bm.Size())

	bm.Close()

	bm, err = openOrCreateFileInode(file)
	assert.Nil(t, err)
	assert.NotNil(t, bm)

	assert.Equal(t, uint64(1024), bm.Size())
}

func TestResizeBlockSize(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	file := dir + "/file"

	bm, err := openOrCreateFileInode(file)
	assert.Nil(t, err)
	assert.NotNil(t, bm)

	bm.Resize(1024)

	assert.Equal(t, uint64(1), bm.SizeBlocks())

	bm.Resize(4096)

	assert.Equal(t, uint64(1), bm.SizeBlocks())

	bm.Resize(4097)

	assert.Equal(t, uint64(2), bm.SizeBlocks())
}

func TestOutOfRangeBlocksAreUnavailable(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	file := dir + "/file"

	bm, err := openOrCreateFileInode(file)
	assert.Nil(t, err)
	assert.NotNil(t, bm)

	assert.False(t, bm.IsAvailable(0))
	assert.False(t, bm.IsAvailable(1))
}

func TestBlocksAreUnavailableByDefault(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	file := dir + "/file"

	bm, err := openOrCreateFileInode(file)
	assert.Nil(t, err)
	assert.NotNil(t, bm)

	bm.Resize(1)

	assert.False(t, bm.IsAvailable(0))
}

func TestSetWrittenMakesAvailable(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	file := dir + "/file"

	bm, err := openOrCreateFileInode(file)
	assert.Nil(t, err)
	assert.NotNil(t, bm)

	bm.Resize(1)

	bm.SetWritten(0, 1)

	assert.Equal(t, true, bm.IsAvailable(0))
}

func TestSetWrittenMakesAvailablePersistence(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	file := dir + "/file"

	bm, err := openOrCreateFileInode(file)
	assert.Nil(t, err)
	assert.NotNil(t, bm)

	bm.Resize(1)
	bm.SetWritten(0, 1)
	bm.Close()

	bm, err = openOrCreateFileInode(file)
	assert.Nil(t, err)
	assert.NotNil(t, bm)

	assert.Equal(t, true, bm.IsAvailable(0))

	bm.Close()
}

func TestSetReadMakesAvailable(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	file := dir + "/file"

	bm, err := openOrCreateFileInode(file)
	assert.Nil(t, err)
	assert.NotNil(t, bm)

	bm.Resize(1)

	bm.SetRead(0, 1)

	assert.Equal(t, true, bm.IsAvailable(0))
}

func TestDiscardMakesUnavailable(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	file := dir + "/file"

	bm, err := openOrCreateFileInode(file)
	assert.Nil(t, err)
	assert.NotNil(t, bm)

	bm.Resize(1)

	bm.SetWritten(0, 1)
	bm.Discard(0, 1)

	assert.Equal(t, false, bm.IsAvailable(0))
}

func TestResizeDiscardsBlocks(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	file := dir + "/file"

	bm, err := openOrCreateFileInode(file)
	assert.Nil(t, err)
	assert.NotNil(t, bm)

	bm.Resize(4096 * 4)
	bm.SetRead(1, 3)

	discarded := bm.Resize(1)
	assert.Equal(t, uint64(2), discarded)
}

func TestTruncateRead(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	file := dir + "/file"

	bm, err := openOrCreateFileInode(file)
	assert.Nil(t, err)
	assert.NotNil(t, bm)

	new_len := bm.TruncateRead(23, 4096)
	assert.Equal(t, uint64(0), new_len)
}

func TestTruncateReadToAvailableBlocks(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	file := dir + "/file"

	bm, err := openOrCreateFileInode(file)
	assert.Nil(t, err)
	assert.NotNil(t, bm)

	bm.Resize(4096 * 2)
	bm.SetRead(0, 1)

	new_len := bm.TruncateRead(23, 4096)
	assert.Equal(t, uint64(4096-23), new_len)
}

func TestTruncateReadToAvailableBlocks2(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	file := dir + "/file"

	bm, err := openOrCreateFileInode(file)
	assert.Nil(t, err)
	assert.NotNil(t, bm)

	bm.Resize(4096 * 4)
	bm.SetRead(0, 2)

	new_len := bm.TruncateRead(23, 8192)
	assert.Equal(t, uint64(8192-23), new_len)
}

func TestResizeToNonMultipleOfBlockSize(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	file := dir + "/file"

	bm, err := openOrCreateFileInode(file)
	assert.Nil(t, err)
	assert.NotNil(t, bm)

	bm.Resize(4096*4 + 1024)
	bm.SetWritten(0, 5)

	assert.True(t, bm.IsAvailable(4))
}

func TestTruncateReadToActualSizeOnLastBlock(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	file := dir + "/file"

	bm, err := openOrCreateFileInode(file)
	assert.Nil(t, err)
	assert.NotNil(t, bm)

	bm.Resize(1024)
	bm.SetWritten(0, 1)

	new_len := bm.TruncateRead(0, 4096)
	assert.Equal(t, uint64(1024), new_len)
}

func TestMarksLastBlockAsMissingOnExtension(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	file := dir + "/file"

	bm, err := openOrCreateFileInode(file)
	assert.Nil(t, err)
	assert.NotNil(t, bm)

	bm.Resize(4096 + 1024)
	bm.SetWritten(0, 2)

	discarded := bm.Resize(4096 + 1025)

	assert.True(t, bm.IsAvailable(0))
	assert.False(t, bm.IsAvailable(1))
	assert.Equal(t, uint64(1), discarded)
}

func TestLastBlockStaysValidOnShrink(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	file := dir + "/file"

	bm, err := openOrCreateFileInode(file)
	assert.Nil(t, err)
	assert.NotNil(t, bm)

	bm.Resize(4096 + 1024)
	bm.SetWritten(0, 2)

	discarded := bm.Resize(4096 + 1023)

	assert.True(t, bm.IsAvailable(0))
	assert.True(t, bm.IsAvailable(1))
	assert.Equal(t, uint64(0), discarded)
}

func TestSetWrittenIncreasesBlocks(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	file := dir + "/file"

	bm, err := openOrCreateFileInode(file)
	assert.Nil(t, err)
	assert.NotNil(t, bm)

	bm.Resize(4096 * 10)

	assert.Equal(t, uint64(0), bm.Blocks())

	bm.SetWritten(0, 1)

	bm.Close()

	bm, err = openOrCreateFileInode(file)
	assert.Nil(t, err)
	assert.NotNil(t, bm)

	assert.Equal(t, uint64(1), bm.Blocks())

	bm.SetWritten(0, 5)

	assert.Equal(t, uint64(5), bm.Blocks())
}

func TestDiscardReducesBlocks(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	file := dir + "/file"

	bm, err := openOrCreateFileInode(file)
	assert.Nil(t, err)
	assert.NotNil(t, bm)

	bm.Resize(4096 * 10)

	bm.SetWritten(0, 5)

	discarded := bm.Discard(2, 7)

	assert.Equal(t, uint64(3), discarded)
	assert.Equal(t, uint64(2), bm.Blocks())

	bm.Close()

	bm, err = openOrCreateFileInode(file)
	assert.Nil(t, err)
	assert.NotNil(t, bm)

	assert.Equal(t, uint64(2), bm.Blocks())
}

func TestSetWrittenClipsAtBlockSize(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	file := dir + "/file"

	bm, err := openOrCreateFileInode(file)
	assert.Nil(t, err)
	assert.NotNil(t, bm)

	bm.Resize(1)

	assert.Equal(t, uint64(0), bm.Blocks())

	bm.SetWritten(0, 4096)

	assert.Equal(t, uint64(1), bm.Blocks())
	assert.False(t, bm.IsAvailable(1))
}

func TestSetReadClipsAtBlockSize(t *testing.T) {
	dir := prepTempDir()
	defer teardownTempDir(dir)

	file := dir + "/file"

	bm, err := openOrCreateFileInode(file)
	assert.Nil(t, err)
	assert.NotNil(t, bm)

	bm.Resize(1)

	assert.Equal(t, uint64(0), bm.Blocks())

	bm.SetRead(0, 4096)

	assert.Equal(t, uint64(1), bm.Blocks())
	assert.False(t, bm.IsAvailable(1))
}
