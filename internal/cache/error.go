package cache

import (
	"syscall"

	"github.com/horazont/dragonstash/internal/backend"
)

func IsUnavailableError(error backend.Error) bool {
	return error.Errno() == uintptr(syscall.EIO)
}
