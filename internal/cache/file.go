package cache

import (
	"io/ioutil"
	"os"
	"path/filepath"
)

type safeFile struct {
	*os.File
	targetName string
}

func CreateSafe(path string) (*safeFile, error) {
	dir := filepath.Dir(path)
	f, err := ioutil.TempFile(dir, ".safe")
	if err != nil {
		return nil, err
	}

	return &safeFile{
		File:       f,
		targetName: path,
	}, nil
}

func OpenSafe(path string) (*safeFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &safeFile{File: f, targetName: ""}, nil
}

func (m *safeFile) Abort() error {
	return m.File.Close()
}

func (m *safeFile) Close() error {
	oldname := m.File.Name()
	err := m.File.Sync()
	if err != nil {
		return err
	}
	err = m.File.Close()
	if err != nil {
		return err
	}
	if m.targetName != "" {
		err = os.Rename(oldname, m.targetName)
	}
	return err
}
