package filecache

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/horazont/dragonstash/internal/layer"
)

var (
	ErrMagicMismatch = errors.New("magic number mismatch")
)

var (
	inode_MAGIC     = [3]byte{0x69, 0x6e, 0x6f}
	inode_DIR_MAGIC = [3]byte{0x44, 0x49, 0x52}
	inode_REG_MAGIC = [3]byte{0x52, 0x45, 0x47}
	inode_LNK_MAGIC = [3]byte{0x4c, 0x4e, 0x4b}
	// FIXME: set this to 4096 - len(inode)
	inode_MAX_LINK_DEST_LEN = uint32(2048)
	inode_MAX_DIR_CHILDREN  = uint32(65535)
	inode_MAX_DIR_ENTRY     = uint32(1024)
)

func checkMagic(val []byte, ref []byte) bool {
	if len(val) != len(ref) {
		return false
	}
	for i, v := range val {
		if v != ref[i] {
			return false
		}
	}
	return true
}

func readVerAndMagic(reader io.Reader, magic_ref []byte) (ver uint8, err error) {
	magic_buf := [3]byte{}
	if _, err = io.ReadFull(reader, magic_buf[:]); err != nil {
		return 0, err
	}
	if !checkMagic(magic_buf[:], magic_ref) {
		return 0, ErrMagicMismatch
	}

	if err = binary.Read(reader, binary.LittleEndian, &ver); err != nil {
		return 0, err
	}

	return ver, nil
}

func writeVerAndMagic(writer io.Writer, ver uint8, magic []byte) error {
	if _, err := writer.Write(magic); err != nil {
		return err
	}

	if err := binary.Write(writer, binary.LittleEndian, &ver); err != nil {
		return err
	}

	return nil
}

func writeLenString(writer io.Writer, s string) error {
	slen := uint32(len(s))
	if err := binary.Write(writer, binary.LittleEndian, &slen); err != nil {
		return err
	}

	if _, err := io.WriteString(writer, s); err != nil {
		return err
	}

	return nil
}

func readLenString(reader io.Reader, max_len uint32) (string, error) {
	var slen uint32
	if err := binary.Read(reader, binary.LittleEndian, &slen); err != nil {
		return "", err
	}
	if slen > max_len {
		return "", errors.New("string too long")
	}

	buf := make([]byte, slen)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return "", err
	}

	return string(buf), nil
}

type inode interface {
	layer.FileStat
	SetMtime(new uint64)
	SetAtime(new uint64)
	SetCtime(new uint64)
	SetSize(new uint64)
	SetOwnerUID(new uint32)
	SetOwnerGID(new uint32)
	SetMode(new uint32)

	Mutex() *sync.Mutex

	Chown(uid uint32, gid uint32)
	Chmod(perms uint32)
	Utimens(mtime *time.Time, atime *time.Time)

	// Write pending changes to the backing storage
	Sync() error

	// Write pending changes and release memory / handles
	Close() error
}

func updateInode(src layer.FileStat, dest inode) {
	dest.SetMode(src.Mode())
	dest.SetMtime(src.Mtime())
	dest.SetAtime(src.Atime())
	dest.SetCtime(src.Ctime())
	dest.SetOwnerUID(src.OwnerUID())
	dest.SetOwnerGID(src.OwnerGID())
	dest.SetSize(src.Size())
}

type baseInode struct {
	storage_path   string
	is_deleted     bool
	mutex          sync.Mutex
	mode           uint32
	mtime          uint64
	atime          uint64
	ctime          uint64
	times_modified bool
	size           uint64
	uid            uint32
	gid            uint32
	perms_modified bool
}

func (m *baseInode) Atime() uint64 {
	return m.atime
}

func (m *baseInode) Blocks() uint64 {
	return 0
}

func (m *baseInode) Close() error {
	return m.Sync()
}

func (m *baseInode) Chmod(perms uint32) {
	mask := uint32(syscall.S_IRWXU | syscall.S_IRWXG | syscall.S_IRWXO)
	m.mode = (m.mode &^ mask) | (perms & mask)
}

func (m *baseInode) Chown(uid uint32, gid uint32) {
	m.uid = uid
	m.gid = gid
}

func (m *baseInode) Ctime() uint64 {
	return m.ctime
}

func (m *baseInode) Mode() uint32 {
	return m.mode
}

func (m *baseInode) Mtime() uint64 {
	return m.mtime
}

func (m *baseInode) Mutex() *sync.Mutex {
	return &m.mutex
}

func (m *baseInode) OwnerGID() uint32 {
	return m.gid
}

func (m *baseInode) OwnerUID() uint32 {
	return m.uid
}

func (m *baseInode) SetAtime(new uint64) {
	m.atime = new
}

func (m *baseInode) SetCtime(new uint64) {
	m.ctime = new
}

func (m *baseInode) SetMode(new uint32) {
	m.Chmod(new)
}

func (m *baseInode) SetMtime(new uint64) {
	m.mtime = new
}

func (m *baseInode) SetOwnerGID(new uint32) {
	m.gid = new
}

func (m *baseInode) SetOwnerUID(new uint32) {
	m.uid = new
}

func (m *baseInode) SetSize(new uint64) {
	m.size = new
}

func (m *baseInode) Size() uint64 {
	return m.size
}

func (m *baseInode) Sync() error {
	return layer.WrapError(syscall.ENOSYS)
}

func (m *baseInode) Utimens(mtime *time.Time, atime *time.Time) {
	m.mtime = uint64(mtime.Unix())
	m.atime = uint64(atime.Unix())
}

func (m *baseInode) read(reader io.Reader) error {
	ver, err := readVerAndMagic(reader, inode_MAGIC[:])
	if err != nil {
		return err
	}

	if ver != 1 {
		return errors.New(fmt.Sprintf("unsupported version: %d", ver))
	}

	// now read the individual fields

	if err = binary.Read(reader, binary.LittleEndian, &m.mode); err != nil {
		return err
	}

	if err = binary.Read(reader, binary.LittleEndian, &m.uid); err != nil {
		return err
	}

	if err = binary.Read(reader, binary.LittleEndian, &m.gid); err != nil {
		return err
	}

	if err = binary.Read(reader, binary.LittleEndian, &m.perms_modified); err != nil {
		return err
	}

	if err = binary.Read(reader, binary.LittleEndian, &m.mtime); err != nil {
		return err
	}

	if err = binary.Read(reader, binary.LittleEndian, &m.atime); err != nil {
		return err
	}

	if err = binary.Read(reader, binary.LittleEndian, &m.ctime); err != nil {
		return err
	}

	if err = binary.Read(reader, binary.LittleEndian, &m.times_modified); err != nil {
		return err
	}

	if err = binary.Read(reader, binary.LittleEndian, &m.size); err != nil {
		return err
	}

	return nil
}

func (m *baseInode) write(writer io.Writer) error {
	if err := writeVerAndMagic(writer, 1, inode_MAGIC[:]); err != nil {
		return err
	}

	// enow write the individual fields, starting with mode
	if err := binary.Write(writer, binary.LittleEndian, &m.mode); err != nil {
		return err
	}

	if err := binary.Write(writer, binary.LittleEndian, &m.uid); err != nil {
		return err
	}

	if err := binary.Write(writer, binary.LittleEndian, &m.gid); err != nil {
		return err
	}

	if err := binary.Write(writer, binary.LittleEndian, &m.perms_modified); err != nil {
		return err
	}

	if err := binary.Write(writer, binary.LittleEndian, &m.mtime); err != nil {
		return err
	}

	if err := binary.Write(writer, binary.LittleEndian, &m.atime); err != nil {
		return err
	}

	if err := binary.Write(writer, binary.LittleEndian, &m.ctime); err != nil {
		return err
	}

	if err := binary.Write(writer, binary.LittleEndian, &m.times_modified); err != nil {
		return err
	}

	if err := binary.Write(writer, binary.LittleEndian, &m.size); err != nil {
		return err
	}

	return nil
}

type linkInode struct {
	baseInode
	dest string
}

func (m *linkInode) Dest() string {
	return m.dest
}

func (m *linkInode) SetDest(new string) {
	m.dest = new
}

func (m *linkInode) Sync() error {
	file, err := CreateSafe(m.storage_path)
	if err != nil {
		return err
	}
	defer file.Abort()

	if err = m.baseInode.write(file); err != nil {
		return err
	}

	if err = writeVerAndMagic(file, 1, inode_LNK_MAGIC[:]); err != nil {
		return err
	}

	if err = writeLenString(file, m.dest); err != nil {
		return err
	}

	file.Close()
	return nil
}

func (m *linkInode) Close() error {
	return m.Sync()
}

func (m *linkInode) readLinkData(reader io.Reader) error {
	ver, err := readVerAndMagic(reader, inode_LNK_MAGIC[:])
	if err != nil {
		return err
	}
	if ver != 1 {
		return errors.New(fmt.Sprintf("unsupported version: %d", ver))
	}

	dest, err := readLenString(reader, inode_MAX_LINK_DEST_LEN)
	if err != nil {
		return err
	}
	m.dest = dest

	return nil
}

type dirInode struct {
	baseInode
	children []string
}

func (m *dirInode) Sync() error {
	file, err := CreateSafe(m.storage_path)
	if err != nil {
		return err
	}
	defer file.Abort()

	if err = m.baseInode.write(file); err != nil {
		return err
	}

	if err = writeVerAndMagic(file, 1, inode_DIR_MAGIC[:]); err != nil {
		return err
	}

	nchildren := uint32(len(m.children))
	if err = binary.Write(file, binary.LittleEndian, &nchildren); err != nil {
		return err
	}

	for _, child := range m.children {
		child_len := uint32(len(child))

		if err = binary.Write(file, binary.LittleEndian, &child_len); err != nil {
			return err
		}

		if _, err = io.WriteString(file, child); err != nil {
			return err
		}
	}

	file.Close()
	return nil
}

func (m *dirInode) Close() error {
	err := m.Sync()
	if err != nil {
		return err
	}
	m.children = nil
	return nil
}

func (m *dirInode) readDirData(reader io.Reader) error {
	ver, err := readVerAndMagic(reader, inode_DIR_MAGIC[:])
	if err != nil {
		return err
	}
	if ver != 1 {
		return errors.New(fmt.Sprintf("unsupported version: %d", ver))
	}

	var nchildren uint32
	if err := binary.Read(reader, binary.LittleEndian, &nchildren); err != nil {
		return err
	}

	if nchildren > inode_MAX_DIR_CHILDREN {
		return errors.New(fmt.Sprintf("too many directory children: %d",
			nchildren))
	}

	m.children = make([]string, nchildren)
	for child_i := uint32(0); child_i < nchildren; child_i++ {
		child, err := readLenString(reader, inode_MAX_DIR_ENTRY)
		if err != nil {
			return err
		}
		m.children[child_i] = child
	}

	return nil
}

func createInode(storage_path string, ref layer.FileStat) (inode, error) {
	base := baseInode{
		storage_path: storage_path,
		mode:         ref.Mode(),
		mtime:        ref.Mtime(),
		atime:        ref.Atime(),
		ctime:        ref.Ctime(),
		size:         ref.Size(),
		uid:          ref.OwnerUID(),
		gid:          ref.OwnerGID(),
	}

	switch base.mode & syscall.S_IFMT {
	case syscall.S_IFLNK:
		result := &linkInode{
			base,
			"",
		}
		return result, nil
	case syscall.S_IFDIR:
		result := &dirInode{
			base,
			nil,
		}
		return result, nil
	case syscall.S_IFREG:
		file, err := os.OpenFile(storage_path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}
		result := &fileInode{
			baseInode: base,
			file:      file,
		}
		// force size to 0 and resize to the size of the reference in a
		// separate step to make things line up nicely.
		result.size = 0
		result.Resize(ref.Size())
		return result, nil
	}

	return nil, syscall.ENOSYS
}

func createEmptyInode(storage_path string, format uint32) (inode, error) {
	base := baseInode{
		storage_path: storage_path,
		mode:         format,
	}

	switch base.mode & syscall.S_IFMT {
	case syscall.S_IFLNK:
		result := &linkInode{
			base,
			"",
		}
		return result, nil
	case syscall.S_IFDIR:
		result := &dirInode{
			base,
			nil,
		}
		return result, nil
	case syscall.S_IFREG:
		file, err := os.OpenFile(storage_path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}
		result := &fileInode{
			baseInode: base,
			file:      file,
		}
		return result, nil
	}

	return nil, syscall.ENOSYS
}

func openInode(storage_path string) (inode, error) {
	close_file := true

	file, err := os.OpenFile(storage_path, os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	defer func() {
		if close_file {
			file.Close()
		}
	}()

	base := baseInode{
		storage_path: storage_path,
	}
	if err = base.read(file); err != nil {
		return nil, err
	}

	log.Printf("inode mode: %d", base.mode)

	switch base.mode & syscall.S_IFMT {
	case syscall.S_IFLNK:
		result := &linkInode{
			base,
			"",
		}
		if err = result.readLinkData(file); err != nil {
			return nil, err
		}
		return result, nil
	case syscall.S_IFDIR:
		result := &dirInode{
			base,
			nil,
		}
		if err = result.readDirData(file); err != nil {
			return nil, err
		}
		return result, nil
	case syscall.S_IFREG:
		result := &fileInode{
			baseInode: base,
			file:      file,
		}
		if err = result.readFileData(file); err != nil {
			return nil, err
		}
		// disable closing of the file on exit
		close_file = false
		return result, nil
	}

	return nil, syscall.ENOSYS
}
