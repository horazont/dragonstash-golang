package cache

import (
	"syscall"

	"github.com/horazont/dragonstash/internal/layer"
)

func IsUnavailableError(error layer.Error) bool {
	return error.Errno() == uintptr(syscall.EIO)
}
