//go:build linux
// +build linux

package buffer_pool_manager

import (
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func OpenFileDirectIO(filePath string, flags int, permissions os.FileMode) (*os.File, error) {

	fd, err := unix.Open(filePath, flags|syscall.O_DIRECT, uint32(permissions))

	if err != nil {
		return nil, err
	}

	return os.NewFile(uintptr(fd), filePath), nil
}
