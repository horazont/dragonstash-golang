package cache

import (
	"log"
	"syscall"

	"github.com/horazont/dragonstash/internal/backend"
)

type CacheLayer struct {
	cache Interface
	fs    backend.FileSystem
}

func NewCacheLayer(cache Interface, fs backend.FileSystem) *CacheLayer {
	return &CacheLayer{
		cache: cache,
		fs:    fs,
	}
}

func (m *CacheLayer) IsReady() bool {
	return true
}

func (m *CacheLayer) Join(elems ...string) string {
	return m.fs.Join(elems...)
}

func (m *CacheLayer) Lstat(path string) (backend.FileStat, backend.Error) {
	if !m.fs.IsReady() {
		return m.cache.FetchAttr(path)
	}
	stat, err := m.fs.Lstat(path)
	if err == nil {
		m.cache.PutAttr(path, stat)
	} else {
		// FIXME: check for connectivity errors and fall back to cache
		// instead of deleting it
		m.cache.Delete(path)
	}
	return stat, err
}

func (m *CacheLayer) OpenDir(path string) ([]backend.DirEntry, backend.Error) {
	if !m.fs.IsReady() {
		return m.cache.FetchDir(path)
	} else {
		entries, err := m.fs.OpenDir(path)
		// we don’t cache errors, for now
		// FIXME: check for connectivity errors
		if err != nil {
			m.cache.Delete(path)
			return entries, err
		}

		m.cache.PutDir(path, m.fs, entries)
		return entries, err
	}
}

func (m *CacheLayer) Readlink(path string) (string, backend.Error) {
	if !m.fs.IsReady() {
		return m.cache.FetchLink(path)
	} else {
		dest, err := m.fs.Readlink(path)
		if err == nil {
			m.cache.PutLink(path, dest)
		} else {
			// FIXME: check for connectivity errors and fall back to
			// cache
			m.cache.Delete(path)
		}
		return dest, err
	}
}

func (m *CacheLayer) OpenFile(path string, flags int) (backend.File, backend.Error) {
	f, err := m.fs.OpenFile(path, flags)
	if err != nil && !IsUnavailableError(err) {
		return f, err
	}

	// stat, err := f.Stat()
	// if err != nil {
	// 	f.Release()
	// 	if !IsUnavailableError(err) {
	// 		return nil, err
	// 	}
	// 	f = nil
	// }

	cachef, err := m.cache.OpenForStore(path, 0, 0)
	if err != nil {
		log.Printf("failed to open cache store for %#v: %s",
			path,
			err,
		)
	}

	if f == nil && cachef == nil {
		return nil, backend.WrapError(syscall.EIO)
	}

	return wrapFile(cachef, f, m.cache.BlockSize()), nil
}

type CacheLayerFile struct {
	blocksize int64
	cacheside CachedFileHandle
	fsside    backend.File
}

func wrapFile(cacheside CachedFileHandle, fsside backend.File, blocksize int64) backend.File {
	return &CacheLayerFile{
		blocksize: blocksize,
		cacheside: cacheside,
		fsside:    fsside,
	}
}

func alignRead(
	position int64,
	length int64,
	blocksize int64,
) (new_position int64, new_length int64, offset int64) {
	new_position = position
	new_length = length
	offset = 0
	if shift := position % blocksize; shift != 0 {
		offset += shift
		new_length += shift
		new_position -= shift
	}
	if add := new_length % blocksize; add != 0 {
		new_length += blocksize - add
	}
	return new_position, new_length, offset
}

func (m *CacheLayerFile) Read(dest []byte, position int64) (int, backend.Error) {
	if m.cacheside == nil {
		return m.fsside.Read(dest, position)
	}

	if m.fsside == nil {
		return m.cacheside.ReadData(dest, position)
	}

	new_position, new_length, offset := alignRead(
		position,
		int64(len(dest)),
		m.blocksize,
	)

	need_copy := new_length != int64(len(dest))
	var buffer []byte = dest
	if need_copy {
		buffer = make([]byte, new_length)
	}

	n, err := m.fsside.Read(buffer, new_position)
	if err != nil {
		if IsUnavailableError(err) {
			// read data from cache instead
			return m.cacheside.ReadData(dest, position)
		} else {
			// read error, do not cache the data
			// TODO: un-cache any cached data in that range
			return n, err
		}
	}
	m.cacheside.PutReadData(buffer[:n], new_position, int64(n) < new_length)

	start := offset
	end := offset + int64(len(dest))
	if start > int64(n) {
		start = 0
		end = 0
		n = 0
	} else if end > int64(n) {
		end = int64(n)
		n = int(end - start)
	}

	copy(dest, buffer[start:end])

	return n, err
}

func (m *CacheLayerFile) Release() {
	log.Printf("releasing cache layer file")

	if m.cacheside != nil {
		m.cacheside.Close()
		m.cacheside = nil
	}

	if m.fsside != nil {
		m.fsside.Release()
		m.fsside = nil
	}
}
