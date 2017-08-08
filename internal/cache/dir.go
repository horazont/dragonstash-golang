package cache

import "github.com/horazont/dragonstash/internal/backend"

type dirCacheEntry struct {
	NameV  string `toml:"name"`
	ModeV  uint32 `toml:"mode"`
	MtimeV uint64 `toml:"mtime"`
	CtimeV uint64 `toml:"ctime"`
	AtimeV uint64 `toml:"atime"`
	SizeV  uint64 `toml:"size"`
	UidV   uint32 `toml:"uid"`
	GidV   uint32 `toml:"gid"`
}

type dirCache struct {
	Entries []dirCacheEntry `toml:"entries"`
}

func fullDirEntryToCache(name string, stat backend.FileStat, dest *dirCacheEntry) {
	dest.NameV = name
	dest.ModeV = stat.Mode()
	dest.MtimeV = stat.Mtime()
	dest.AtimeV = stat.Atime()
	dest.CtimeV = stat.Ctime()
	dest.SizeV = stat.Size()
	dest.UidV = stat.OwnerUID()
	dest.GidV = stat.OwnerGID()
}

func dirEntryToCache(path string, fs backend.FileSystem, entry backend.DirEntry, dest *dirCacheEntry) bool {
	var err error = nil
	stat := entry.Stat()
	if stat == nil {
		stat, err = fs.Lstat(fs.Join(path, entry.Name()))
		if err != nil {
			return false
		}
	}
	fullDirEntryToCache(entry.Name(), stat, dest)
	return true
}

func dirEntriesToCache(path string, fs backend.FileSystem, entries []backend.DirEntry) []dirCacheEntry {
	var dest = 0
	result := make([]dirCacheEntry, len(entries))
	for _, entry := range entries {
		var ok bool
		ok = dirEntryToCache(path, fs, entry, &result[dest])
		if ok {
			dest += 1
		}
	}
	return result[:dest]
}

func (m *dirCacheEntry) Name() string {
	return m.NameV
}

func (m *dirCacheEntry) Mode() uint32 {
	return m.ModeV
}

func (m *dirCacheEntry) Stat() backend.FileStat {
	return m
}

func (m *dirCacheEntry) Atime() uint64 {
	return m.AtimeV
}

func (m *dirCacheEntry) Blocks() uint64 {
	return 0
}

func (m *dirCacheEntry) Ctime() uint64 {
	return m.CtimeV
}

func (m *dirCacheEntry) Mtime() uint64 {
	return m.MtimeV
}

func (m *dirCacheEntry) OwnerGID() uint32 {
	return m.GidV
}

func (m *dirCacheEntry) OwnerUID() uint32 {
	return m.UidV
}

func (m *dirCacheEntry) Size() uint64 {
	return m.SizeV
}
