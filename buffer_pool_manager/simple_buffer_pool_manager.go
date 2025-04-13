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

	// page ID of the page currently stored in the frame.
	pageId PageID

	// stores page data.
	data []byte

	// pinCount keeps track of the number of threads currently accessing/using the page.
	pinCount int

	// used to keep track of whether the data field has been written to since it was read from disk.
	dirty bool

	// used by the page guards to determine whether the page has been altered.
	version int

	// used to synchronize access to the page and its meta data stored in the frame.
	mutex *sync.RWMutex
}
type SimpleBufferPoolManager struct {

	// Replacer is a component used by the Buffer Pool Manager to decide which page to evict
	// when there are no free frames available.
	// It tracks unpinned pages and provides a policy (e.g., LRU, Clock)
	// for selecting a victim frame for replacement.
	replacer Replacer

	// DiskManager handles all interactions with the disk.
	// It is responsible for allocating and deallocating page IDs,
	// reading pages from disk into memory, and writing pages from memory back to disk.
	// It also manages metadata such as the list of deallocated page IDs and the next available page ID.
	disk *DiskManager

	pageTableMutex *sync.RWMutex

	// pageTable is used to map page IDs to frame IDs.
	// It is used to keep track of which page is currently stored in which frame.
	pageTable map[PageID]FrameID

	// fixed size array of frames.
	frames []Frame

	// used to keep track of empty frames.
	freeFrames []FrameID

	// used to synchronize access to the list of free frames.
	frameAllocationMutex *sync.Mutex

	// size of the frames array
	size int
}

func NewSimpleBufferPoolManager(size int, replacer Replacer, disk *DiskManager) *SimpleBufferPoolManager {

	frames := make([]Frame, 0)

	for i := range frames {
		frames[i] = Frame{}
	}

	freeFrames := make([]FrameID, 0)

	for i := 0; i < size; i++ {
		freeFrames = append(freeFrames, FrameID(i))
	}

	return &SimpleBufferPoolManager{
		replacer: replacer,
		disk:     disk,

		pageTableMutex: &sync.RWMutex{},
		pageTable:      make(map[PageID]FrameID),
		frames:         frames,

		frameAllocationMutex: &sync.Mutex{},
		freeFrames:           freeFrames,
		size:                 size,
	}
}

// NewPage is a thread-safe function that allocates a new page in the file, and returns its page ID.
func (bufferPool SimpleBufferPoolManager) NewPage() PageID {

	return bufferPool.disk.allocatePage()
}

// fetchPage returns a pointer to the frame storing the page with a given page ID.
// DO NOT call fetchPage directly, as it is not thread-safe.
// Always used a page guard to access page data.
func (bufferPool SimpleBufferPoolManager) fetchPage(pageId PageID) (*Frame, error) {

	bufferPool.pageTableMutex.RLock()
	frameId, exists := bufferPool.pageTable[pageId]
	bufferPool.pageTableMutex.RUnlock()

	// --> If page table entry does not exist:
	if !exists {

		// 1. Read page from file using disk manager.
		data, err := bufferPool.disk.read(int64(pageId*PAGE_SIZE), PAGE_SIZE)
		if err != nil {
			return nil, err
		}

		var frameId FrameID

		// 2. Check if free frame exists in free frame list.
		if len(bufferPool.freeFrames) > 0 {

			bufferPool.frameAllocationMutex.Lock()
			frameId = bufferPool.freeFrames[0]
			bufferPool.freeFrames = bufferPool.freeFrames[1:]
			bufferPool.pageTableMutex.Unlock()

		} else {
			// 3.0 If free frame list is empty, evict a frame using replacer.

			frameId = bufferPool.replacer.victim()

			// 3.1 If frame to be evicted is dirty, write to disk.

			bufferPool.pageTableMutex.Lock()
			frame := bufferPool.frames[frameId]

			if frame.dirty {
				bufferPool.disk.write(int64(frame.pageId*PAGE_SIZE), frame.data)
			}

		}

		// 4. Store page data in frame with pin count = 1

		frame := bufferPool.frames[frameId]

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
	frame := bufferPool.frames[frameId]

	// 2. If pin count of frame = 0, remove frame from LRU replacer.
	if frame.pinCount == 0 {
		bufferPool.replacer.remove(frameId)
	}

	// 3. Increment pin count.
	frame.pinCount++

	// 4. Return data stored in frame.
	return &frame, nil

}

// deletePage is used to deallocate a page which contains data that is no longer useful.
// DO NOT call deletePage directly, as it is not thread-safe.
// always call the DeletePage function of the write guard corresponding to a page, to safely delete it.
func (bufferPool SimpleBufferPoolManager) deletePage(pageId PageID) (bool, error) {

	// 1. Fetch frameId corresponding to pageId.
	frameId, exists := bufferPool.pageTable[pageId]

	// 2. If page not in memory, return false.
	if !exists {
		return false, nil
	}

	// 3. Fetch frame.
	frame := bufferPool.frames[frameId]

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

	// 6. Add frameId to freeFrames.
	bufferPool.freeFrames = append(bufferPool.freeFrames, frameId)

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

// used to decrement the pin count of a page.
func (bufferPool SimpleBufferPoolManager) unpinPage(pageId PageID) bool {

	// 1. Fetch frameId corresponding to pageId.
	frameId, exists := bufferPool.pageTable[pageId]

	// 2. If page not in memory, return false.
	if !exists {
		return false
	}

	// 3. Fetch frame.
	frame := bufferPool.frames[frameId]

	// 4. Decrement pin count.
	frame.pinCount--

	// 5. If pin count = 0, add frame to replacer.
	if frame.pinCount == 0 {
		bufferPool.replacer.insert(frameId)
	}

	return true
}

// flushPage is used to write page to disk, currently used when a dirty page must be evicted from the buffer pool.
func (bufferPool SimpleBufferPoolManager) flushPage(pageId PageID) (bool, error) {

	frameId, exists := bufferPool.pageTable[pageId]

	if !exists {
		return false, nil
	}

	frame := bufferPool.frames[frameId]

	if frame.dirty {
		return true, bufferPool.disk.write(int64(pageId*PAGE_SIZE), frame.data)
	}

	return true, nil
}

// flushAllPages is used to write all dirty pages to disk, currently used during database shutdown.
func (bufferPool SimpleBufferPoolManager) flushAllPages() error {

	for pageId, frameId := range bufferPool.pageTable {

		frame := bufferPool.frames[frameId]

		if frame.dirty {

			if err := bufferPool.disk.write(int64(pageId*PAGE_SIZE), frame.data); err != nil {
				return err
			}
		}
	}

	return nil
}

// Close must be executed to ensure correct shutdown of buffer pool manager.
func (bufferPool SimpleBufferPoolManager) Close() error {

	if err := bufferPool.flushAllPages(); err != nil {
		return err
	}

	if err := bufferPool.disk.Close(); err != nil {
		return err
	}

	return nil

}
