package filecache

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"unsafe"

	mmap "github.com/edsrzf/mmap-go"
)

var (
	ErrInvalidFileInodeVersion = errors.New("Invalid fileInode version")
)

const (
	fileInode_HEADER_SIZE     = 128
	fileInode_PAGE_SIZE       = 4096
	fileInode_BLOCK_INFO_SIZE = 2
)

const (
	block_FLAG_DIRTY = (1 << 15)
	block_FLAG_RSVD0 = (1 << 14)
	block_FLAG_RSVD1 = (1 << 13)
	block_FLAG_RSVD2 = (1 << 12)
	block_FLAGS      = (block_FLAG_DIRTY | block_FLAG_RSVD0 |
		block_FLAG_RSVD1 | block_FLAG_RSVD2)

	block_ACTR_MASK = 0x00ff
	block_ACTR_MAX  = 255
)

type blockinfo uint16

func (m *blockinfo) writeACTR(value uint8) {
	*m = blockinfo((uint16(*m) &^ block_ACTR_MASK) | uint16(value))
}

func (m blockinfo) readACTR() uint8 {
	return uint8(uint16(m) & block_ACTR_MASK)
}

func (m *blockinfo) writeFlags(value uint16) {
	*m = blockinfo((uint16(*m) &^ block_FLAGS) | (value & block_FLAGS))
}

func (m blockinfo) readFlags() uint16 {
	return uint16(m) & block_FLAGS
}

// Increase the access counter
//
// Access counters are saturating, i.e. they do not wrap around but instead stay
// at their maximum value.
//
// Return values:
// - new: True if the counter was zero before the Touch
// - overflow: True if the counter is now at its maximum value
func (m *blockinfo) Touch() (new bool, overflow bool) {
	ctr := m.readACTR()
	if ctr == block_ACTR_MAX {
		return false, true
	}
	new = ctr == 0
	ctr += 1
	m.writeACTR(ctr)
	overflow = ctr == block_ACTR_MAX
	return new, overflow
}

// Divide the value of the access counter by two, but ensure that the value is
// non-zero if it was non-zero before.
func (m *blockinfo) Shift() {
	ctr := m.readACTR()
	if ctr == 0 {
		return
	}
	ctr >>= 1
	if ctr == 0 {
		ctr = 1
	}
	m.writeACTR(ctr)
}

// Discard a block
//
// Return true if the block was previously available
func (m *blockinfo) Discard() (existed bool) {
	existed = m.readACTR() != 0
	*m = blockinfo(0)
	return existed
}

func (m *blockinfo) MarkDirty() {
	m.writeFlags(m.readFlags() | block_FLAG_DIRTY)
}

func (m blockinfo) IsDirty() bool {
	return m.readFlags()&block_FLAG_DIRTY != 0
}

func (m blockinfo) IsAvailable() bool {
	return m.readACTR() != 0
}

type fileInode struct {
	baseInode
	blocks_used uint64
	file        *os.File
	handle      *fileCachedFile
	blockmmap   mmap.MMap
	blockmap    []blockinfo
}

func openOrCreateFileInode(storage_path string) (result *fileInode, err error) {
	is_new := false
	file, err := os.OpenFile(storage_path, os.O_RDWR, 0600)
	if err != nil {
		file, err = os.OpenFile(storage_path, os.O_EXCL|os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}
		is_new = true
	}

	defer func() {
		if err != nil {
			file.Close()
		}
	}()

	result = &fileInode{
		file: file,
	}
	if !is_new {
		if err = result.baseInode.read(file); err != nil {
			return nil, err
		}
		if err = result.readFileData(file); err != nil {
			return nil, err
		}
	}

	return result, nil
}

func (m *fileInode) readFileData(reader io.Reader) error {
	ver, err := readVerAndMagic(reader, inode_REG_MAGIC[:])
	if err != nil {
		return err
	}
	if ver != 1 {
		return errors.New(fmt.Sprintf("unsupported version: %d", ver))
	}

	if err = binary.Read(reader, binary.LittleEndian, &m.blocks_used); err != nil {
		return err
	}

	return nil
}

func (m *fileInode) writeFileData(writer io.Writer) error {
	if err := writeVerAndMagic(writer, 1, inode_REG_MAGIC[:]); err != nil {
		return err
	}

	if err := binary.Write(writer, binary.LittleEndian, &m.blocks_used); err != nil {
		return err
	}

	return nil
}

func (m *fileInode) ensureMapped() {
	if m.blockmmap != nil {
		return
	}
	var err error
	if m.file == nil {
		panic(fmt.Sprintf("fileInode backing file is nil!"))
	}
	m.blockmmap, err = mmap.Map(m.file, mmap.RDWR, 0)
	if err != nil {
		panic(fmt.Sprintf("failed to map fileInode into memory (size=%d, backingSize=%d): %s",
			m.size,
			m.backingSize(),
			err))
	}
	m.blockmap =
		(*(*[]blockinfo)(unsafe.Pointer(&m.blockmmap)))[fileInode_HEADER_SIZE/fileInode_BLOCK_INFO_SIZE:]
}

func (m *fileInode) ensureUnmapped() {
	if m.blockmmap == nil {
		return
	}
	m.blockmap = nil
	m.blockmmap.Flush()
	m.blockmmap.Unmap()
	m.blockmmap = nil
}

func (m *fileInode) backingSize() uint64 {
	stat, err := m.file.Stat()
	if err != nil {
		panic("failed to stat fileInode")
	}
	return uint64(stat.Size())
}

func (m *fileInode) resizeMapToBlocks(new_blocks uint64) {
	curr_size := m.backingSize()
	new_size := new_blocks*fileInode_BLOCK_INFO_SIZE + fileInode_HEADER_SIZE
	// align to full page, because that’s what’s going to be allocated
	// anyways
	new_pages := (new_size + fileInode_PAGE_SIZE - 1) / fileInode_PAGE_SIZE
	new_size = new_pages * fileInode_PAGE_SIZE
	log.Printf("fileInode: resizing to %d blocks => %d pages => %d bytes",
		new_blocks,
		new_pages,
		new_size)
	if curr_size == new_size {
		return
	}
	m.ensureUnmapped()
	m.file.Truncate(int64(new_size))
}

func (m *fileInode) IsAvailable(block uint64) bool {
	if block >= m.SizeBlocks() {
		return false
	}
	m.ensureMapped()
	return m.blockmap[block].IsAvailable()
}

func (m *fileInode) SetWritten(start uint64, end uint64) {
	nblocks := m.SizeBlocks()
	if start >= nblocks {
		return
	}
	if end <= start {
		return
	}
	if end > nblocks {
		end = nblocks
	}
	m.ensureMapped()
	var new_blocks uint64
	for i := start; i < end; i++ {
		new, _ := m.blockmap[i].Touch()
		if new {
			new_blocks += 1
		}
	}
	m.blocks_used += new_blocks
}

// Return the number of blocks discarded. This may be less than the number of
// blocks in the range if some blocks were unavailable.
func (m *fileInode) Discard(start uint64, end uint64) uint64 {
	if start >= m.SizeBlocks() {
		return 0
	}
	if end <= start {
		return 0
	}
	m.ensureMapped()
	var ctr uint64
	for i := start; i < end; i++ {
		if m.blockmap[i].Discard() {
			ctr += 1
		}
	}
	m.blocks_used -= uint64(ctr)
	return ctr
}

func (m *fileInode) SetRead(start uint64, end uint64) {
	m.SetWritten(start, end)
}

func (m *fileInode) getAvailableBlocks(start uint64, end uint64) uint64 {
	var ctr uint64 = 0
	for i := start; i < end; i++ {
		if m.blockmap[i].IsAvailable() {
			ctr += 1
		}
	}
	return ctr
}

// Return the number of blocks which were discarded
func (m *fileInode) Resize(nbytes uint64) (discarded uint64) {
	new_blocks := (nbytes + BLOCK_SIZE - 1) / BLOCK_SIZE
	old_size := m.Size()
	old_blocks := m.SizeBlocks()
	log.Printf(
		"Resize: old_size=%d, old_blocks=%d, new_size=%d, new_blocks=%d",
		old_size, old_blocks,
		nbytes, new_blocks,
	)
	if new_blocks < old_blocks {
		m.ensureMapped()
		discarded = m.getAvailableBlocks(new_blocks, old_blocks)
	} else if nbytes > old_size && old_size > 0 && old_size%BLOCK_SIZE != 0 {
		m.ensureMapped()
		// discard the last block if it was available and file size wasn’t aligned
		if m.blockmap[old_blocks-1].Discard() {
			discarded = 1
		}
	}
	m.size = nbytes
	m.resizeMapToBlocks(new_blocks)
	if err := m.writeMetadata(); err != nil {
		panic(fmt.Sprintf("failed to write metadata: %s", err))
	}
	return discarded
}

func (m *fileInode) SetSize(new uint64) {
	if new != m.size {
		log.Printf("WARNING: SetSize() used with fileInode; this isn’t handled yet properly")
	}
	m.Resize(new)
}

func (m *fileInode) SizeBlocks() uint64 {
	return (m.size + BLOCK_SIZE - 1) / BLOCK_SIZE
}

func (m *fileInode) writeMetadata() error {
	_, err := m.file.Seek(0, os.SEEK_SET)
	if err != nil {
		return err
	}
	if err = m.baseInode.write(m.file); err != nil {
		return err
	}
	if err = m.writeFileData(m.file); err != nil {
		return err
	}
	return nil
}

func (m *fileInode) Sync() error {
	m.ensureUnmapped()
	if err := m.writeMetadata(); err != nil {
		return err
	}
	return m.file.Sync()
}

func (m *fileInode) Close() error {
	if err := m.Sync(); err != nil {
		return err
	}
	m.file.Close()
	return nil
}

// Truncate a given read to the maximum available range of data
func (m *fileInode) TruncateRead(position uint64, size uint64) (actual_size uint64, at_eof bool) {
	filesize := m.Size()
	log.Printf("TruncateRead: position=%d, size=%d",
		position, size)
	if filesize == 0 {
		// cannot map, bail out early
		return 0, true
	}

	m.ensureMapped()

	start_block := position / BLOCK_SIZE
	end_byte := position + size
	// truncating here saves us from a possibly expensive linear scan over
	// non-existant blocks
	if end_byte > filesize {
		log.Printf("truncating at eof (%d)", filesize)
		end_byte = filesize
		size = end_byte - position
		at_eof = true
	}
	end_block := (position + size + BLOCK_SIZE - 1) / BLOCK_SIZE
	actual_end_block := end_block

	for block := start_block; block < end_block; block++ {
		if !m.blockmap[block].IsAvailable() {
			actual_end_block = block
			log.Printf("block %d not available, truncating here", block)
			at_eof = false
			break
		}
	}

	if actual_end_block <= start_block {
		return 0, false
	}

	actual_end_byte := actual_end_block * BLOCK_SIZE

	if actual_end_byte > end_byte {
		actual_end_byte = end_byte
	}
	if actual_end_byte > filesize {
		actual_end_byte = filesize
		at_eof = true
	}
	actual_size = actual_end_byte - position
	log.Printf("TruncateRead: position=%d, size=%d -> %d",
		position, size, actual_size)
	return actual_size, at_eof
}

func (m *fileInode) Blocks() uint64 {
	if m.SizeBlocks() == 0 {
		return 0
	}
	return m.blocks_used
}
