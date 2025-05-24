package buffer_pool_manager

import (
	"log"
	"sync"
)

const (
	PAGE_SIZE        = 4096
	FREELIST_PAGE_ID = 0
)

type FrameID int
type PageID uint64

type BufferPoolManager interface {

	// public methods

	// NewPage allocates a new page in the file and returns its page ID.
	NewPage() PageID

	NewWriteGuard(pageId PageID) (*WriteGuard, error)
	NewReadGuard(pageId PageID) (*ReadGuard, error)

	// Close is called during shutdown to ensure data durability.
	// It flushes all dirty pages to disk, writes the free list metadata page,
	// and closes the underlying file.
	Close() error

	// private methods

	// flushAllPages writes all dirty pages currently in the buffer pool to disk.
	flushAllPages() error

	// fetchPage loads a page with the given page ID into the buffer pool,
	// returning the corresponding frame. If the page is already in memory,
	// it returns the cached frame.
	fetchPage(pageID PageID) (*Frame, error)

	// deletePage removes a page with the given page ID from both memory and disk.
	// Returns true if the deletion was successful.
	deletePage(pageID PageID) (bool, error)

	// unpinPage marks a page as no longer being used by any client.
	// If the page is dirty, it remains in the buffer pool until flushed.
	unpinPage(pageID PageID) bool
}

type Frame struct {

	// page ID of the page currently stored in the frame.
	pageId PageID

	// stores page data.
	data []byte

	// synchronizes access to the pinCount variable.
	pinCountMutex *sync.Mutex

	// pinCount keeps track of the number of threads currently accessing/using the page.
	pinCount int

	// used to keep track of whether the data field has been written to since it was read from disk.
	dirty bool

	// used to synchronize access to the page and its metadata stored in the frame.
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
	disk DiskManager

	lookupMutex *sync.RWMutex

	// pageTable is used to map page IDs to frame IDs.
	// It is used to keep track of which page is currently stored in which frame.
	pageTable map[PageID]FrameID

	// fixed size array of frames.
	frames []*Frame

	// used to keep track of empty frames.
	freeFrames []FrameID

	// used to synchronize access to the list of free frames.
	frameAllocationMutex *sync.Mutex

	// size of each page in the file
	pageSize int

	// size of the frames array
	poolSize int
}

func NewSimpleBufferPoolManager(poolSize int, pageSize int, replacer Replacer, disk DiskManager) *SimpleBufferPoolManager {

	frames := make([]*Frame, poolSize)

	for i := range frames {
		frames[i] = &Frame{
			mutex:         &sync.RWMutex{},
			pinCountMutex: &sync.Mutex{},
		}
	}

	freeFrames := make([]FrameID, 0)

	for i := 0; i < poolSize; i++ {
		freeFrames = append(freeFrames, FrameID(i))
	}

	return &SimpleBufferPoolManager{
		replacer: replacer,
		disk:     disk,

		lookupMutex: &sync.RWMutex{},
		pageTable:   make(map[PageID]FrameID),
		frames:      frames,

		frameAllocationMutex: &sync.Mutex{},
		freeFrames:           freeFrames,
		poolSize:             poolSize,
		pageSize:             pageSize,
	}
}

// NewPage is a thread-safe function that allocates a new page in the file, and returns its page ID.
func (bufferPool *SimpleBufferPoolManager) NewPage() PageID {

	return bufferPool.disk.allocatePage()
}

// fetchPage returns a pointer to the frame storing the page with a given page ID.
// DO NOT call fetchPage directly, as it is not thread-safe.
// Always use a page guard to access page data.
func (bufferPool *SimpleBufferPoolManager) fetchPage(pageId PageID) (*Frame, error) {

	bufferPool.lookupMutex.RLock()

	frameId, exists := bufferPool.pageTable[pageId]

	if exists {

		frame := bufferPool.frames[frameId]

		frame.pinCountMutex.Lock()

		frame.pinCount++

		if frame.pinCount == 1 {

			bufferPool.replacer.remove(frameId)
		}

		frame.pinCountMutex.Unlock()

		bufferPool.lookupMutex.RUnlock()

		return frame, nil
	}

	bufferPool.lookupMutex.RUnlock()

	bufferPool.lookupMutex.Lock()
	defer bufferPool.lookupMutex.Unlock()

	frameId, exists = bufferPool.pageTable[pageId]

	if exists {

		frame := bufferPool.frames[frameId]

		frame.pinCountMutex.Lock()

		frame.pinCount++

		if frame.pinCount == 1 {
			bufferPool.replacer.remove(frameId)
		}
		frame.pinCountMutex.Unlock()

		return frame, nil

	}

	data, err := bufferPool.disk.read(int64(pageId)*int64(bufferPool.pageSize), bufferPool.pageSize)

	if err != nil {
		return nil, err
	}

	bufferPool.frameAllocationMutex.Lock()

	var newFrameId FrameID
	if len(bufferPool.freeFrames) > 0 {

		newFrameId = bufferPool.freeFrames[0]
		log.Printf("free frame chosen => %d", newFrameId)
		bufferPool.freeFrames = bufferPool.freeFrames[1:]
		log.Printf("free frame list => %v", bufferPool.freeFrames)
	} else {
		newFrameId = bufferPool.replacer.victim()

		frame := bufferPool.frames[newFrameId]

		delete(bufferPool.pageTable, frame.pageId)
		if frame.dirty {

			// handle error correctly.
			if err := bufferPool.disk.write(int64(frame.pageId)*int64(bufferPool.pageSize), frame.data); err != nil {
				return nil, err
			}
		}

	}

	bufferPool.frameAllocationMutex.Unlock()

	frame := bufferPool.frames[newFrameId]

	frame.data = data
	frame.pinCount = 1
	frame.pageId = pageId
	frame.dirty = false

	bufferPool.pageTable[pageId] = newFrameId

	return frame, nil

}

// deletePage is used to deallocate a page which contains data that is no longer useful.
// DO NOT call deletePage directly, as it is not thread-safe.
// always call the DeletePage function of the write guard corresponding to a page, to safely delete it.
func (bufferPool *SimpleBufferPoolManager) deletePage(pageId PageID) (bool, error) {

	bufferPool.lookupMutex.Lock()
	defer bufferPool.lookupMutex.Unlock()

	// 1. Fetch frameId corresponding to pageId.
	frameId, exists := bufferPool.pageTable[pageId]

	// 2. If page not in memory, return false.
	if !exists {
		return false, nil
	}

	// 3. Fetch frame.
	frame := bufferPool.frames[frameId]

	frame.pinCountMutex.Lock()

	// 4. If page is being used by others, it cannot be deleted, so return false.
	if frame.pinCount != 1 {
		frame.pinCountMutex.Unlock()
		return false, nil

	}
	frame.pinCountMutex.Unlock()

	// 5. If frame stores a dirty page, write to disk.
	if frame.dirty {

		if err := bufferPool.disk.write(int64(frame.pageId)*int64(bufferPool.pageSize), frame.data); err != nil {
			return true, err
		}
	}

	bufferPool.frameAllocationMutex.Lock()
	// 6. Add frameId to freeFrames.
	bufferPool.freeFrames = append(bufferPool.freeFrames, frameId)
	bufferPool.frameAllocationMutex.Unlock()

	// 7. Delete page table entry.
	delete(bufferPool.pageTable, pageId)

	// 8. Deallocate page in file.
	bufferPool.disk.deallocatePage(pageId)

	// 9. Free up space.
	frame.data = nil

	// 10. Reset dirty, version, pageId fields of the frame
	frame.dirty = false
	frame.pageId = 0
	frame.pinCount = 0

	return true, nil
}

// unpinPage is used to decrement the pin count of a page.
func (bufferPool *SimpleBufferPoolManager) unpinPage(pageId PageID) bool {

	bufferPool.lookupMutex.RLock()
	defer bufferPool.lookupMutex.RUnlock()

	// 1. Fetch frameId corresponding to pageId.
	frameId, exists := bufferPool.pageTable[pageId]

	// 2. If page not in memory, return false.
	if !exists {
		return false
	}

	// 3. Fetch frame.
	frame := bufferPool.frames[frameId]

	frame.pinCountMutex.Lock()

	// 4. Decrement pin count.
	frame.pinCount--

	// 5. If pin count = 0, add frame to replacer.
	if frame.pinCount == 0 {
		bufferPool.replacer.insert(frameId)
	}

	frame.pinCountMutex.Unlock()

	return true
}

// flushAllPages is used to write all dirty pages to disk, currently used during database shutdown.
func (bufferPool *SimpleBufferPoolManager) flushAllPages() error {

	bufferPool.lookupMutex.RLock()
	defer bufferPool.lookupMutex.RUnlock()

	for pageId, frameId := range bufferPool.pageTable {

		frame := bufferPool.frames[frameId]

		if frame.dirty {

			if err := bufferPool.disk.write(int64(pageId)*int64(bufferPool.pageSize), frame.data); err != nil {
				return err
			}
		}
	}

	return nil
}

// Close must be executed to ensure correct shutdown of buffer pool manager.
func (bufferPool *SimpleBufferPoolManager) Close() error {

	if err := bufferPool.flushAllPages(); err != nil {
		return err
	}

	if err := bufferPool.disk.close(); err != nil {
		return err
	}

	return nil

}
