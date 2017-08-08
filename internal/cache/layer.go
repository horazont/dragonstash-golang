package cache

import "github.com/horazont/dragonstash/internal/backend"

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
	}
	return stat, err
}

func (m *CacheLayer) OpenDir(path string) ([]backend.DirEntry, backend.Error) {
	if !m.fs.IsReady() {
		return m.cache.FetchDir(path)
	} else {
		entries, err := m.fs.OpenDir(path)
		// we don’t cache errors, for now
		if err != nil {
			m.cache.DelDir(path)
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
		}
		return dest, err
	}
}

func (m *CacheLayer) OpenFile(path string, flags int) (backend.File, backend.Error) {
	return m.fs.OpenFile(path, flags)
}
