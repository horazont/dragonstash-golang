package cache

// This is used for testing.
import "io"

type OSFileSystem interface {
	Open(path string) (OSFile, error)
}

type OSFile interface {
	io.Reader
	io.ReaderAt
	io.Writer
	io.WriterAt
}
