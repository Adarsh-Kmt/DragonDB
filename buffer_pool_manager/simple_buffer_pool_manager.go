package buffer_pool_manager

import "sync"

const (
	PAGE_SIZE        = 4096
	FREELIST_PAGE_ID = 0
	DIRTY            = true
	CLEAN            = false
)

type FrameID int
type PageID uint64

type BufferPoolManager interface {
	NewPage() PageID

	flushPage(pageID PageID) (bool, error)
	flushAllPages() error

	fetchPage(pageID PageID) (*Frame, error)

	deletePage(pageID PageID) (bool, error)
	unpinPage(pageID PageID) bool

	Close() error
}

type Frame struct {
	pageId PageID
	data   []byte

	pinCount int
	dirty    bool

	version int
	mutex   *sync.RWMutex
}
type SimpleBufferPoolManager struct {
	replacer Replacer
	disk     *DiskManager

	pageMutex *sync.RWMutex
	pageTable map[PageID]FrameID
	frameList []Frame

	frameAllocationMutex *sync.Mutex
	freeFrameList        []FrameID
	size                 int
}

func NewSimpleBufferPoolManager(size int, replacer Replacer, disk *DiskManager) *SimpleBufferPoolManager {

	frameList := make([]Frame, 0)

	for i := range frameList {
		frameList[i] = Frame{}
	}

	freeFrameList := make([]FrameID, 0)

	for i := 0; i < size; i++ {
		freeFrameList = append(freeFrameList, FrameID(i))
	}

	return &SimpleBufferPoolManager{
		replacer: replacer,
		disk:     disk,

		pageMutex: &sync.RWMutex{},
		pageTable: make(map[PageID]FrameID),
		frameList: frameList,

		frameAllocationMutex: &sync.Mutex{},
		freeFrameList:        freeFrameList,
		size:                 size,
	}
}

func (bufferPool *SimpleBufferPoolManager) NewPage() PageID {

	return bufferPool.disk.allocatePage()
}

func (bufferPool *SimpleBufferPoolManager) fetchPage(pageId PageID) (*Frame, error) {

	frameId, exists := bufferPool.pageTable[pageId]

	// --> If page table entry does not exist:
	if !exists {

		// 1. Read page from file using disk manager.
		data, err := bufferPool.disk.read(int64(pageId*PAGE_SIZE), PAGE_SIZE)
		if err != nil {
			return nil, err
		}

		var frameId FrameID

		// 2. Check if free frame exists in free frame list.
		if len(bufferPool.freeFrameList) > 0 {

			frameId = bufferPool.freeFrameList[0]
			bufferPool.freeFrameList = bufferPool.freeFrameList[1:]

		} else {
			// 3.0 If free frame list is empty, evict a frame using replacer.

			frameId = bufferPool.replacer.victim()

			// 3.1 If frame to be evicted is dirty, write to disk.

			frame := bufferPool.frameList[frameId]

			if frame.dirty {
				bufferPool.disk.write(int64(frame.pageId*PAGE_SIZE), frame.data)
			}

		}

		// 4. Store page data in frame with pin count = 1

		frame := bufferPool.frameList[frameId]

		frame.data = data
		frame.pageId = pageId
		frame.pinCount = 1
		frame.dirty = false

		// 5. Update page table.
		bufferPool.pageTable[pageId] = frameId

		return &frame, nil
	}

	// --> If page table entry exists:

	// 1. Fetch frameId corresponding to pageId.
	frame := bufferPool.frameList[frameId]

	// 2. If pin count of frame = 0, remove frame from LRU replacer.
	if frame.pinCount == 0 {
		bufferPool.replacer.remove(frameId)
	}

	// 3. Increment pin count.
	frame.pinCount++

	// 4. Return data stored in frame.
	return &frame, nil

}

func (bufferPool SimpleBufferPoolManager) deletePage(pageId PageID) (bool, error) {

	// 1. Fetch frameId corresponding to pageId.
	frameId, exists := bufferPool.pageTable[pageId]

	// 2. If page not in memory, return false.
	if !exists {
		return false, nil
	}

	// 3. Fetch frame.
	frame := bufferPool.frameList[frameId]

	// 4. If page is being used by others, it cannot be deleted, so return false.
	if frame.pinCount != 1 {
		return false, nil

	}

	// 5. If frame stores a dirty page, write to disk.
	if frame.dirty {
		if err := bufferPool.disk.write(int64(pageId*PAGE_SIZE), frame.data); err != nil {
			return true, err
		}
	}

	// 6. Add frameId to freeFrameList.
	bufferPool.freeFrameList = append(bufferPool.freeFrameList, frameId)

	// 7. Delete page table entry.
	delete(bufferPool.pageTable, pageId)

	// 8. Deallocate page in file.
	bufferPool.disk.deallocatePage(pageId)

	// 9. Free up space.
	frame.data = nil

	// 10. Reset dirty, version, pageId fields of the frame
	frame.dirty = false
	frame.version = 1
	frame.pageId = 0

	return true, nil
}

func (bufferPool SimpleBufferPoolManager) unpinPage(pageId PageID) bool {

	// 1. Fetch frameId corresponding to pageId.
	frameId, exists := bufferPool.pageTable[pageId]

	// 2. If page not in memory, return false.
	if !exists {
		return false
	}

	// 3. Fetch frame.
	frame := bufferPool.frameList[frameId]

	// 4. Decrement pin count.
	frame.pinCount--

	// 5. If pin count = 0, add frame to replacer.
	if frame.pinCount == 0 {
		bufferPool.replacer.insert(frameId)
	}

	return true
}

func (bufferPool SimpleBufferPoolManager) flushPage(pageId PageID) (bool, error) {

	frameId, exists := bufferPool.pageTable[pageId]

	if !exists {
		return false, nil
	}

	frame := bufferPool.frameList[frameId]

	if frame.dirty {
		return true, bufferPool.disk.write(int64(pageId*PAGE_SIZE), frame.data)
	}

	return true, nil
}

func (bufferPool SimpleBufferPoolManager) flushAllPages() error {

	for pageId, frameId := range bufferPool.pageTable {

		frame := bufferPool.frameList[frameId]

		if frame.dirty {

			if err := bufferPool.disk.write(int64(pageId*PAGE_SIZE), frame.data); err != nil {
				return err
			}
		}
	}

	return nil
}

func (bufferPool SimpleBufferPoolManager) Close() error {

	if err := bufferPool.flushAllPages(); err != nil {
		return err
	}

	if err := bufferPool.disk.Close(); err != nil {
		return err
	}

	return nil

}
