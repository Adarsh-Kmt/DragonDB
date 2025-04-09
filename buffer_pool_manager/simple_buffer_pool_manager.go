package buffer_pool_manager

import "encoding/binary"

const (
	PAGE_SIZE        = 4096
	FREELIST_PAGE_ID = 0
	DIRTY            = true
	CLEAN            = false
)

type BufferPoolManager interface {
	ReadPage(pageId uint64) ([]byte, error)
	WritePage(pageId uint64, data []byte) error

	AllocatePage() uint64
	DeallocatePage(pageId uint64)

	UnpinPage(pageId uint64)
	PinPage(pageId uint64)
}

type SimpleBufferPoolManager struct {
	cache Cache
	disk  *DiskManager

	releasedPageIds    []uint64
	maxAllocatedPageId uint64
}

func NewSimpleBufferPoolManager(cache Cache, disk *DiskManager) *SimpleBufferPoolManager {

	freelistPageData, err := disk.Read(FREELIST_PAGE_ID*PAGE_SIZE, PAGE_SIZE)
	if err != nil {
		panic(err)
	}

	maxAllocatedPageId, releasedPageIds := deserializeFreelistPage(freelistPageData)

	return &SimpleBufferPoolManager{
		cache:              cache,
		disk:               disk,
		releasedPageIds:    releasedPageIds,
		maxAllocatedPageId: maxAllocatedPageId,
	}
}
func (pool *SimpleBufferPoolManager) ReadPage(pageId uint64, pinPage bool) ([]byte, error) {

	// check if page exists in data.
	data, exists := pool.cache.RetrievePage(pageId)

	if exists {
		if pinPage {
			pool.cache.PinPage(pageId)
		}
		return data, nil
	}

	// read page data from disk.
	data, err := pool.disk.Read(int64(pageId*PAGE_SIZE), PAGE_SIZE)

	if err != nil {
		return nil, err
	}

	// cache page that was read from disk
	dirtyPageId, dirtyPageData := pool.cache.CachePage(pageId, data, CLEAN)

	//
	pool.cache.PinPage(pageId)
	// if a dirty page was evicted to make room for the current page,
	// write it to disk.
	if dirtyPageId != nil {
		err = pool.disk.Write(int64(*dirtyPageId), dirtyPageData)
		if err != nil {
			return data, err
		}
	}
	return data, nil

}

func (pool *SimpleBufferPoolManager) WritePage(pageId uint64, data []byte) error {

	dirtyPageId, dirtyPageData := pool.cache.CachePage(pageId, data, DIRTY)

	if dirtyPageId != nil {

		err := pool.disk.Write(int64(*dirtyPageId*PAGE_SIZE), dirtyPageData)
		if err != nil {
			return err
		}
	}

	return nil
}

func (pool *SimpleBufferPoolManager) AllocatePage() uint64 {

	if len(pool.releasedPageIds) == 0 {
		pageId := pool.maxAllocatedPageId + 1
		pool.maxAllocatedPageId += 1
		return pageId
	} else {
		pageId := pool.releasedPageIds[0]
		pool.releasedPageIds = pool.releasedPageIds[1:]
		return pageId
	}
}

func (pool *SimpleBufferPoolManager) DeallocatePage(pageId uint64) {
	pool.releasedPageIds = append(pool.releasedPageIds, pageId)
}

func (pool *SimpleBufferPoolManager) Close() error {

	freelistPageData := serializeFreelistPage(pool.maxAllocatedPageId, pool.releasedPageIds)

	err := pool.disk.Write(int64(PAGE_SIZE*FREELIST_PAGE_ID), freelistPageData)

	if err != nil {
		return err
	}

	dirtyPages := pool.cache.GetDirtyPages()

	for pageId, data := range dirtyPages {

		if err = pool.disk.Write(int64(pageId*PAGE_SIZE), data); err != nil {
			return err
		}
	}

	err = pool.disk.file.Close()

	if err != nil {
		return err
	}

	return nil
}

func deserializeFreelistPage(data []byte) (maxAllocatedPageId uint64, releasedPages []uint64) {

	pointer := 0
	maxAllocatedPageId = binary.LittleEndian.Uint64(data[pointer : pointer+8])

	pointer += 8

	releasedPageListSize := binary.LittleEndian.Uint64(data[pointer : pointer+8])
	pointer += 8

	releasedPages = make([]uint64, 0)

	for i := 0; i < int(releasedPageListSize); i++ {
		releasedPages = append(releasedPages, binary.LittleEndian.Uint64(data[pointer:pointer+8]))
		pointer += 8
	}

	return maxAllocatedPageId, releasedPages
}

func serializeFreelistPage(maxAllocatedPageId uint64, releasedPages []uint64) []byte {

	data := make([]byte, 0)

	pointer := 0
	binary.LittleEndian.PutUint64(data[pointer:pointer+8], maxAllocatedPageId)
	pointer += 8

	binary.LittleEndian.PutUint64(data[pointer:pointer+8], uint64(len(releasedPages)))
	pointer += 8

	for _, pageId := range releasedPages {
		binary.LittleEndian.PutUint64(data[pointer:pointer+8], pageId)
		pointer += 8
	}
	return data

}
