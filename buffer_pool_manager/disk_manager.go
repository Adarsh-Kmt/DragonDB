package buffer_pool_manager

import (
	"encoding/binary"
	"fmt"
	"os"
)

type DiskManager struct {
	file *os.File

	deallocatedPageIdList []PageID
	maxAllocatedPageId    PageID
}

func NewDiskManager(filePath string) (*DiskManager, error) {

	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return nil, err
	}

	disk := &DiskManager{file: f}

	freeListPageData, err := disk.read(FREELIST_PAGE_ID*PAGE_SIZE, PAGE_SIZE)

	if err != nil {
		return nil, err
	}

	disk.deserializeFreelistPage(freeListPageData)

	return disk, nil
}
func (disk *DiskManager) write(offset int64, data []byte) error {

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

func (disk *DiskManager) read(offset int64, size int) ([]byte, error) {

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

func (disk *DiskManager) allocatePage() PageID {

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

func (disk *DiskManager) deallocatePage(pageId PageID) {
	disk.deallocatedPageIdList = append(disk.deallocatedPageIdList, pageId)
}

func (disk *DiskManager) Close() error {

	freelistPageData := disk.serializeFreelistPage()

	if err := disk.write(FREELIST_PAGE_ID*PAGE_SIZE, freelistPageData); err != nil {
		return err
	}

	if err := disk.file.Close(); err != nil {
		return err
	}

	return nil
}

func (disk *DiskManager) serializeFreelistPage() []byte {

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

func (disk *DiskManager) deserializeFreelistPage(data []byte) {

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
