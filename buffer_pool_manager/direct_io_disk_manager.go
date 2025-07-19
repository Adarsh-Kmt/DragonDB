package buffer_pool_manager

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"

	codec "github.com/Adarsh-Kmt/DragonDB/page_codec"
	"github.com/ncw/directio"
)

// Disk Manager is responsible for reading, writing, allocating and deallocating pages on disk.
type DiskManager interface {

	// write function writes data to a particular offset in the file.
	write(offset int64, data []byte) error

	// reads a specified amount of data starting from a particular offset in the file.
	read(offset int64, size int) ([]byte, error)

	// allocatePage allocates a page in the file and returns a new page ID for use.
	// It reuses a deallocated page ID if available, otherwise increments maxAllocatedPageId and returns a new page ID.
	allocatePage() (uint64, error)

	// deallocatePage marks a page ID as free and adds it to the free list, making it available for future allocation.
	deallocatePage(pageId uint64)

	// writes the serialized metadata page to file, then closes the file.
	close() error
}

// DirectIODiskManager uses Direct I/O to read/write pages of data directly between user process memory and disk controller.

// Direct I/O bypasses the kernel page cache, this is useful because:
// 1. It prevents the file data from being cached twice, once in kernel page cache, and once in database process memory.
// 2. It gives the database complete control over when data is flushed to disk.

type DirectIODiskManager struct {
	file     *os.File
	metadata *codec.MetaData
	codec    codec.MetaDataCodec
	mutex    *sync.Mutex
}

func NewDirectIODiskManager(filePath string) (*DirectIODiskManager, *codec.MetaData, error) {

	fmt.Println()

	// flag represents whether a dragon.db file exists in the given file path or not.
	newFileCreated := false

	// check if a dragon.db file exists in the the current directory.
	if _, err := os.Stat(filePath); errors.Is(err, os.ErrNotExist) {
		slog.Info("dragon.db file does not exist, creating new file...", "filePath", filePath, "function", "NewDirectIODiskManager", "at", "DirectIODiskManager")
		newFileCreated = true
	}

	slog.Info("Opening file in DIRECT I/O mode", "function", "NewDirectIODiskManager", "at", "DirectIODiskManager")

	// Create a dragon.db file if it does not exist, initialize a file descriptor with Direct I/O flag, and read/write permissions.
	file, err := directio.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return nil, nil, err
	}

	disk := &DirectIODiskManager{
		file:  file,
		codec: codec.DefaultMetaDataCodec(),
		mutex: &sync.Mutex{},
	}

	// if a new file had to be created, create a meta data page, and write it to disk.
	if newFileCreated {
		disk.metadata = &codec.MetaData{
			DeallocatedPageIdList: []uint64{},
			MaxAllocatedPageId:    0,
			// root node does not exist
			RootNodePageId: 0,
		}
		slog.Info("writing new metadata page", "function", "NewDirectIODiskManager", "at", "DirectIODiskManager")
		if err = disk.write(METADATA_PAGE_ID*PAGE_SIZE, disk.codec.EncodeMetaDataPage(disk.metadata)); err != nil {
			slog.Error("Failed to write metadata page", "error", err.Error(), "function", "NewDirectIODiskManager", "at", "DirectIODiskManager")
			return nil, nil, err
		}
		slog.Info("New metadata page written successfully", "function", "NewDirectIODiskManager", "at", "DirectIODiskManager")

	} else {
		slog.Info("Reading metadata page from existing file", "function", "NewDirectIODiskManager", "at", "DirectIODiskManager")
		metaDataPage, err := disk.read(METADATA_PAGE_ID*PAGE_SIZE, PAGE_SIZE)

		if err != nil {
			slog.Error("Failed to read metadata page", "error", err.Error(), "function", "NewDirectIODiskManager", "at", "DirectIODiskManager")
			return nil, nil, err
		}

		disk.metadata = disk.codec.DecodeMetaDataPage(metaDataPage)
	}

	return disk, disk.metadata, nil

}

// write function writes data to a particular offset in the file.
func (disk *DirectIODiskManager) write(offset int64, data []byte) error {

	fmt.Println()
	slog.Info("Writing data to offset", "offset", offset, "size", len(data), "function", "write", "at", "DirectIODiskManager")

	_, err := disk.file.Seek(offset, 0)
	if err != nil {
		slog.Error("Failed to seek to offset", "offset", offset, "error", err.Error(), "function", "write", "at", "DirectIODiskManager")
		return err
	}

	n, err := disk.file.Write(data)
	if err != nil {
		slog.Error("Failed to write data", "error", err.Error(), "function", "write", "at", "DirectIODiskManager")
		return err
	}

	if n != len(data) {
		return fmt.Errorf("incomplete write")
	}
	return nil
}

// reads a specified amount of data starting from a particular offset in the file.
func (disk *DirectIODiskManager) read(offset int64, size int) ([]byte, error) {

	fmt.Println()
	slog.Info("Reading data from offset", "offset", offset, "size", size, "function", "read", "at", "DirectIODiskManager")
	_, err := disk.file.Seek(offset, 0)
	if err != nil {
		slog.Info("Failed to seek to offset", "offset", offset, "error", err.Error(), "function", "read", "at", "DirectIODiskManager")
		return nil, err
	}
	slog.Info("allocating aligned block for read", "size", size, "function", "read", "at", "DirectIODiskManager")
	data := directio.AlignedBlock(size)

	n, err := disk.file.Read(data)
	if err != nil {
		slog.Error("Failed to read data", "error", err.Error(), "function", "read", "at", "DirectIODiskManager")
		return nil, err
	}
	if n != size {
		return nil, fmt.Errorf("incomplete read")
	}
	return data, nil

}

// allocatePage allocates a page in the file and returns a new page ID for use.
// It reuses a deallocated page ID if available, otherwise increments maxAllocatedPageId and returns a new page ID.
func (disk *DirectIODiskManager) allocatePage() (uint64, error) {

	fmt.Println()
	disk.mutex.Lock()
	defer disk.mutex.Unlock()

	if len(disk.metadata.DeallocatedPageIdList) > 0 {

		pageId := disk.metadata.DeallocatedPageIdList[0]
		slog.Info(fmt.Sprintf("allocating existing page with page ID = %d", pageId), "function", "allocatePage", "at", "DirectIODiskManager")
		disk.metadata.DeallocatedPageIdList = disk.metadata.DeallocatedPageIdList[1:]
		return pageId, nil
	} else {
		pageId := disk.metadata.MaxAllocatedPageId + 1
		disk.metadata.MaxAllocatedPageId++
		slog.Info(fmt.Sprintf("allocating new page with page ID = %d", pageId), "function", "allocatePage", "at", "DirectIODiskManager")

		err := disk.write(int64(pageId)*PAGE_SIZE, make([]byte, PAGE_SIZE))
		if err != nil {
			slog.Error("Failed to write new page", "pageId", pageId, "error", err.Error(), "function", "allocatePage", "at", "DirectIODiskManager")
			return 0, err
		}
		return pageId, nil
	}
}

// deallocatePage marks a page ID as free and adds it to the free list, making it available for future allocation.
func (disk *DirectIODiskManager) deallocatePage(pageId uint64) {

	fmt.Println()
	slog.Info(fmt.Sprintf("deallocating page with page ID = %d", pageId), "function", "deallocatePage", "at", "DirectIODiskManager")
	disk.mutex.Lock()
	disk.metadata.DeallocatedPageIdList = append(disk.metadata.DeallocatedPageIdList, pageId)
	disk.mutex.Unlock()
}

// writes the serialized metadata page to file, then closes the file.
func (disk *DirectIODiskManager) close() error {

	fmt.Println()
	slog.Info("Closing DirectIODiskManager...", "function", "close", "at", "DirectIODiskManager")
	freelistPageData := disk.codec.EncodeMetaDataPage(disk.metadata)

	slog.Info("Writing metadata page before closing", "function", "close", "at", "DirectIODiskManager")

	if err := disk.write(METADATA_PAGE_ID*PAGE_SIZE, freelistPageData); err != nil {
		slog.Error("Failed to write metadata page", "error", err.Error(), "function", "close", "at", "DirectIODiskManager")
		return err
	}

	if err := disk.file.Close(); err != nil {
		slog.Error("Failed to close file", "error", err.Error(), "function", "close", "at", "DirectIODiskManager")
		return err
	}

	return nil
}
