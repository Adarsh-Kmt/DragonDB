package buffer_pool_manager

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

type OSBufferedDiskManager struct {
	file *os.File

	mutex                 *sync.Mutex
	deallocatedPageIdList []PageID
	maxAllocatedPageId    PageID
}

func NewOSBufferedDiskManager(filePath string) (*OSBufferedDiskManager, error) {

	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return nil, err
	}

	disk := &OSBufferedDiskManager{
		file:  f,
		mutex: &sync.Mutex{},
	}

	freeListPageData, err := disk.read(FREELIST_PAGE_ID*PAGE_SIZE, PAGE_SIZE)

	if err != nil {
		return nil, err
	}

	disk.deserializeFreelistPage(freeListPageData)

	return disk, nil
}

// writes data to a particular offset in the file.
func (disk *OSBufferedDiskManager) write(offset int64, data []byte) error {

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

// reads a specified amount of data starting from a particular offset in the file.
func (disk *OSBufferedDiskManager) read(offset int64, size int) ([]byte, error) {

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

// allocatePage allocates a page in the file and returns a new page ID for use.
// It reuses a deallocated page ID if available, otherwise increments and returns a new page ID.
func (disk *OSBufferedDiskManager) allocatePage() PageID {

	disk.mutex.Lock()
	defer disk.mutex.Unlock()

	if len(disk.deallocatedPageIdList) > 0 {

		pageId := disk.deallocatedPageIdList[0]
		disk.deallocatedPageIdList = disk.deallocatedPageIdList[1:]
		return pageId
	} else {
		pageId := disk.maxAllocatedPageId + 1
		disk.maxAllocatedPageId++
		return pageId
	}
}

// deallocatePage marks a page ID as free and adds it to the free list,
// making it available for future allocation.
func (disk *OSBufferedDiskManager) deallocatePage(pageId PageID) {

	disk.mutex.Lock()
	disk.deallocatedPageIdList = append(disk.deallocatedPageIdList, pageId)
	disk.mutex.Unlock()
}

// writes the serialized freelist page to file, then closes the file.
func (disk *OSBufferedDiskManager) close() error {

	freelistPageData := disk.serializeFreelistPage()

	if err := disk.write(FREELIST_PAGE_ID*PAGE_SIZE, freelistPageData); err != nil {
		return err
	}

	if err := disk.file.Close(); err != nil {
		return err
	}

	return nil
}

// serializeFreeListPage encodes the list of deallocated page IDs and max allocated page ID into a byte slice
// so it can be written to disk. This ensures persistence of the free list across restarts.
func (disk *OSBufferedDiskManager) serializeFreelistPage() []byte {

	data := make([]byte, 0)

	pointer := 0
	binary.LittleEndian.PutUint64(data[pointer:pointer+8], uint64(disk.maxAllocatedPageId))
	pointer += 8

	binary.LittleEndian.PutUint64(data[pointer:pointer+8], uint64(len(disk.deallocatedPageIdList)))
	pointer += 8

	for _, pageId := range disk.deallocatedPageIdList {
		binary.LittleEndian.PutUint64(data[pointer:pointer+8], uint64(pageId))
		pointer += 8
	}
	return data

}

// deserializeFreeListPage decodes the byte slice from disk into the in-memory
// list of deallocated page IDs. This restores the free list after a database restart.
func (disk *OSBufferedDiskManager) deserializeFreelistPage(data []byte) {

	pointer := 0
	disk.maxAllocatedPageId = PageID(binary.LittleEndian.Uint64(data[pointer : pointer+8]))

	pointer += 8

	releasedPageListSize := binary.LittleEndian.Uint64(data[pointer : pointer+8])
	pointer += 8

	deallocatedPageIdList := make([]PageID, 0)

	for i := 0; i < int(releasedPageListSize); i++ {
		deallocatedPageIdList = append(deallocatedPageIdList, PageID(binary.LittleEndian.Uint64(data[pointer:pointer+8])))
		pointer += 8
	}

	disk.deallocatedPageIdList = deallocatedPageIdList
}
