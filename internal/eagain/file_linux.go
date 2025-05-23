//go:build linux

package eagain

import (
	"errors"
	"io"

	"golang.org/x/sys/unix"

	"github.com/lxc/incus/v6/internal/linux"
)

// Reader represents an io.Reader that handles EAGAIN.
type Reader struct {
	Reader io.Reader
}

// Read behaves like io.Reader.Read but will retry on EAGAIN.
func (er Reader) Read(p []byte) (int, error) {
again:
	n, err := er.Reader.Read(p)
	if err == nil {
		return n, nil
	}

	// keep retrying on EAGAIN
	errno, ok := linux.GetErrno(err)
	if ok && (errors.Is(errno, unix.EAGAIN) || errors.Is(errno, unix.EINTR)) {
		goto again
	}

	return n, err
}

// Writer represents an io.Writer that handles EAGAIN.
type Writer struct {
	Writer io.Writer
}

// Write behaves like io.Writer.Write but will retry on EAGAIN.
func (ew Writer) Write(p []byte) (int, error) {
again:
	n, err := ew.Writer.Write(p)
	if err == nil {
		return n, nil
	}

	// keep retrying on EAGAIN
	errno, ok := linux.GetErrno(err)
	if ok && (errors.Is(errno, unix.EAGAIN) || errors.Is(errno, unix.EINTR)) {
		goto again
	}

	return n, err
}
