package backend

import (
	"os"
	"syscall"
)

type wrappedOSError struct {
	cause error
	errno syscall.Errno
}

func WrapError(err error) Error {
	if err == nil {
		return nil
	}

	if err == os.ErrPermission {
		return newWrappedOSError(err, syscall.Errno(syscall.EPERM))
	} else if err == os.ErrNotExist {
		return newWrappedOSError(err, syscall.Errno(syscall.ENOENT))
	} else if err == os.ErrExist {
		return newWrappedOSError(err, syscall.Errno(syscall.EEXIST))
	} else if err == os.ErrInvalid {
		return newWrappedOSError(err, syscall.Errno(syscall.EINVAL))
	}

	switch cast := err.(type) {
	case *os.PathError:
		if syserr, ok := cast.Err.(*syscall.Errno); ok {
			return newWrappedOSError(err, *syserr)
		}
		return WrapError(cast.Err)
	case *syscall.Errno:
		return newWrappedOSError(err, *cast)
	case syscall.Errno:
		return newWrappedOSError(err, cast)
	default:
		return newWrappedOSError(err, syscall.Errno(syscall.EIO))
	}
}

func newWrappedOSError(cause error, errno syscall.Errno) Error {
	return &wrappedOSError{
		cause: cause,
		errno: errno,
	}
}

func (m *wrappedOSError) Error() string {
	if m.cause != nil {
		return m.cause.Error()
	} else {
		return m.errno.Error()
	}
}

func (m *wrappedOSError) Errno() uintptr {
	return uintptr(m.errno)
}

type backendError struct {
	msg   string
	errno uintptr
}

func NewBackendError(msg string, errno syscall.Errno) Error {
	return &backendError{
		msg:   msg,
		errno: uintptr(errno),
	}
}

func (m *backendError) Error() string {
	return m.msg
}

func (m *backendError) Errno() uintptr {
	return m.errno
}
