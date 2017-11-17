package filecache

import (
	"log"
	"os"
	"syscall"

	"github.com/horazont/dragonstash/internal/cache"
	"github.com/horazont/dragonstash/internal/layer"
)

type fileCachedFile struct {
	quota  cache.QuotaService
	inode  *fileInode
	refcnt uint64
	file   *os.File
}

func openFileCachedFile(quota cache.QuotaService, inode *fileInode) (*fileCachedFile, layer.Error) {
	data_path := inode.storage_path + ".data"

	file, err := os.OpenFile(data_path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		log.Printf("failed to open data file: %s", err)
		return nil, layer.WrapError(syscall.EIO)
	}

	return &fileCachedFile{
		quota:  quota,
		inode:  inode,
		refcnt: 1,
		file:   file,
	}, nil
}

func (m *fileCachedFile) lock() {
	m.inode.mutex.Lock()
}

func (m *fileCachedFile) unlock() {
	m.inode.mutex.Unlock()
}

func (m *fileCachedFile) incRef() {
	m.refcnt += 1
}

func (m *fileCachedFile) decRef() {
	m.refcnt -= 1
	if m.refcnt == 0 {
		m.close()
		m.inode.handle = nil
	}
}

func (m *fileCachedFile) close() {
	m.file.Sync()
	m.inode.Sync()
	m.file.Close()
}

func (m *fileCachedFile) IncRef() {
	m.lock()
	defer m.unlock()

	m.incRef()
}

func (m *fileCachedFile) size() uint64 {
	stat, err := m.file.Stat()
	if err != nil {
		panic("failed to stat data file")
	}
	return uint64(stat.Size())
}

func (m *fileCachedFile) discard(start_block uint64, end_block uint64) {
	// FIXME: use proper constants once they are in syscall.
	syscall.Fallocate(
		int(m.file.Fd()),
		0x2|0x1,
		int64(start_block*BLOCK_SIZE),
		int64((end_block-start_block)*BLOCK_SIZE),
	)
	m.inode.Discard(start_block, end_block)
}

func (m *fileCachedFile) resize(new_size uint64, old_size uint64) {
	if old_size < 0 {
		old_size = m.size()
	}
	m.inode.Resize(new_size)
	m.file.Truncate(int64(new_size))
	// FIXME: release discarded blocks
	// FIXME: handle discarding of last block on grow
	// FIXME: make sure the inode is marked dirty
}

func (m *fileCachedFile) writeRandom(data []byte, position uint64) error {
	start_block := position / BLOCK_SIZE
	start_aligned := start_block*BLOCK_SIZE == position
	end_byte := position + uint64(len(data))
	end_block := (end_byte + BLOCK_SIZE - 1) / BLOCK_SIZE
	end_aligned := end_block*BLOCK_SIZE == end_byte

	if !start_aligned && !m.inode.IsAvailable(start_block) {
		// cannot write here because the block is incomplete
		return cache.ErrMustBeAligned
	}

	if !end_aligned && !m.inode.IsAvailable(end_block-1) {
		// cannot write here because the block is incomplete
		return cache.ErrMustBeAligned
	}

	// no resize needed per definition of this operation
	m.writeAndMarkWritten(data, position)

	return nil
}

func (m *fileCachedFile) writeAndMarkWritten(data []byte, position uint64) {
	// FIXME: allocate needed blocks

	end_byte := uint64(len(data)) + position
	end_block := uint64((end_byte + BLOCK_SIZE - 1) / BLOCK_SIZE)
	n, _ := m.file.WriteAt(data, int64(position))
	actual_end_byte := uint64(n) + position
	if actual_end_byte < end_byte {
		// don’t round to full block here, eof handling does not apply
		end_block = actual_end_byte / BLOCK_SIZE
		// make sure the incompletely written block is discarded
		m.discard(end_block, end_block+1)
	}

	m.inode.SetWritten(
		position/BLOCK_SIZE,
		end_block,
	)
}

func (m *fileCachedFile) writeAndExtend(data []byte, position uint64, size uint64) error {
	start_block := position / BLOCK_SIZE
	start_aligned := start_block*BLOCK_SIZE == position
	end_byte := position + uint64(len(data))

	if !start_aligned && !m.inode.IsAvailable(start_block) {
		// cannot write here because the block is incomplete
		return cache.ErrMustBeAligned
	}

	m.resize(end_byte, size)

	m.writeAndMarkWritten(data, position)

	return nil

}

func (m *fileCachedFile) appendToEnd(data []byte, position uint64, size uint64) error {
	m.resize(uint64(len(data))+position, size)
	m.writeAndMarkWritten(data, position)

	return nil
}

func (m *fileCachedFile) PutData(data []byte, position uint64) error {
	m.lock()
	defer m.unlock()

	// three cases:
	//
	// 1. write somewhere inside the file (writeRandom)
	// 2. write beyond the end of file (writeAndExtend)
	// 3. write beyond the end of file starting at the end of file (appendToEnd)
	//
	// special things:
	// (3) should keep the last block valid, also it’ll be unaligned

	// detect which case we have
	start_byte := position
	end_byte := uint64(len(data)) + position
	size := m.size()

	if start_byte == size {
		return m.appendToEnd(data, position, size)
	} else if end_byte >= size {
		return m.writeAndExtend(data, position, size)
	} else {
		return m.writeRandom(data, position)
	}

	return nil
}

func (m *fileCachedFile) FetchData(data []byte, position uint64) (int, error) {
	m.lock()
	defer m.unlock()

	length := uint64(len(data))
	to_read := m.inode.TruncateRead(position, length)

	n, err := m.file.ReadAt(data[:to_read], int64(position))
	if uint64(n) < length {
		if err != nil {
			return n, layer.WrapError(err)
		} else {
			// data not in cache
			return n, layer.WrapError(syscall.EIO)
		}
	}

	return n, err
}

func (m *fileCachedFile) FetchAttr() (layer.FileStat, layer.Error) {
	m.lock()
	defer m.unlock()

	// stat := *m.inode.attr()
	// stat.BlocksV = m.inode.BlocksUsed()

	// FIXME: return a copy here for safety
	return m.inode, nil
}

func (m *fileCachedFile) Chown(uid uint32, gid uint32) layer.Error {
	m.lock()
	defer m.unlock()

	m.inode.Chown(uid, gid)

	return nil
}

func (m *fileCachedFile) Close() {
	m.lock()
	defer m.unlock()

	// may invalidate this
	m.decRef()
}
