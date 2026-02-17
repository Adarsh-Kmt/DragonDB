//go:build darwin
// +build darwin

package bufferpoolmanager

import (
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func OpenFileDirectIOMac(filePath string, flags int, permissions int) (*os.File, error) {

	fd, err := unix.Open(filePath, flags, uint32(permissions))

	if err != nil {
		return nil, err
	}

	file := os.NewFile(uintptr(fd), filePath)

	if _, _, errNum := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_NOCACHE, uintptr(1)); errNum != 0 {

		//fmt.Printf("Syscall to SYS_FCNTL failed\n\tr1=%v, r2=%v, err=%v\n", r1, r2, err)
		file.Close()
		return nil, fmt.Errorf("error while opening file in DIRECT I/O mode")
	}

	return file, nil
}
