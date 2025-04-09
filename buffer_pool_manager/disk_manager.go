package buffer_pool_manager

import (
	"fmt"
	"os"
)

type DiskManager struct {
	file *os.File
}

func NewDiskManager(filePath string) (*DiskManager, error) {

	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os., 0755)
	if err != nil {
		return nil, err
	}
	return &DiskManager{f}, nil
}
func (disk *DiskManager) Write(offset int64, data []byte) error {

	_, err := disk.file.Seek(offset, 0)
	if err != nil {
		return err
	}

	n, err := disk.file.Write(data)
	if err != nil {
		return err
	}

	if n != len(data) {
		return fmt.Errorf("incomplete write")
	}
	return nil
}

func (disk *DiskManager) Read(offset int64, size int) ([]byte, error) {

	_, err := disk.file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}
	data := make([]byte, size)

	n, err := disk.file.Read(data)
	if err != nil {
		return nil, err
	}
	if n != size {
		return nil, fmt.Errorf("incomplete read")
	}
	return data, nil

}
