package buffer_pool_manager

import (
	"fmt"
	"log/slog"
	"sync"

	"golang.org/x/sys/unix"
)

const (
	PAGE_SIZE        = 4096
	METADATA_PAGE_ID = 0
)

type FrameID int

type BufferPoolManager interface {

	// public methods

	// NewPage allocates a new page in the file and returns its page ID.
	NewPage() (uint64, error)

	// If page is allocated in the file, but guard couldn't be acquired, then the allocated page must be added to the deallocatedPageId list.
	CleanupPage(pageID uint64)

	NewWriteGuard(pageId uint64) (*WriteGuard, error)
	NewReadGuard(pageId uint64) (*ReadGuard, error)

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
	fetchPage(pageID uint64) (*Frame, error)

	// deletePage removes a page with the given page ID from both memory and disk.
	// Returns true if the deletion was successful.
	deletePage(pageID uint64) (bool, error)

	// unpinPage marks a page as no longer being used by any client.
	// If the page is dirty, it remains in the buffer pool until flushed.
	unpinPage(pageID uint64) bool
}

type Frame struct {

	// page ID of the page currently stored in the frame.
	pageId uint64

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
	pageTable map[uint64]FrameID

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

func NewSimpleBufferPoolManager(poolSize int, pageSize int, replacer Replacer, disk DiskManager) (*SimpleBufferPoolManager, error) {

	frames := make([]*Frame, poolSize)

	for i := range frames {

		buf, err := createFrameBuffer(pageSize)

		if err != nil {
			return nil, err
		}
		frames[i] = &Frame{
			mutex:         &sync.RWMutex{},
			pinCountMutex: &sync.Mutex{},
			data:          buf,
		}
	}

	freeFrames := make([]FrameID, 0)

	for i := range poolSize {
		freeFrames = append(freeFrames, FrameID(i))
	}

	return &SimpleBufferPoolManager{
		replacer: replacer,
		disk:     disk,

		lookupMutex: &sync.RWMutex{},
		pageTable:   make(map[uint64]FrameID),
		frames:      frames,

		frameAllocationMutex: &sync.Mutex{},
		freeFrames:           freeFrames,
		poolSize:             poolSize,
		pageSize:             pageSize,
	}, nil
}

// NewPage is a thread-safe function that allocates a new page in the file, and returns its page ID.
func (bufferPool *SimpleBufferPoolManager) NewPage() (uint64, error) {

	return bufferPool.disk.allocatePage()
}

// If page is allocated in the file, but guard couldn't be acquired, then the allocated page must be added to the deallocatedPageId list.
func (bufferPool *SimpleBufferPoolManager) CleanupPage(pageID uint64) {

	bufferPool.disk.deallocatePage(pageID)
}

// fetchPage returns a pointer to the frame storing the page with a given page ID.
// DO NOT call fetchPage directly, as it is not thread-safe.
// Always use a page guard to access page data.
func (bufferPool *SimpleBufferPoolManager) fetchPage(pageId uint64) (*Frame, error) {

	bufferPool.lookupMutex.RLock()
	slog.Info(fmt.Sprintf("fetching page %d", pageId), "function", "fetchPage", "at", "buffer Pool Manager")
	slog.Info(fmt.Sprintf("page table => %v", bufferPool.pageTable), "function", "fetchPage", "at", "buffer Pool Manager")
	frameId, exists := bufferPool.pageTable[pageId]
	if exists {

		slog.Info(fmt.Sprintf("page %d found in memory", pageId), "function", "fetchPage", "at", "buffer Pool Manager")
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
		slog.Error("Failed to read page from disk", "pageId", pageId, "error", err.Error(), "function", "fetchPage", "at", "buffer Pool Manager")
		return nil, err
	}

	bufferPool.frameAllocationMutex.Lock()

	var newFrameId FrameID
	if len(bufferPool.freeFrames) > 0 {

		newFrameId = bufferPool.freeFrames[0]

		slog.Info(fmt.Sprintf("free frame chosen => %d", newFrameId), "function", "fetchPage", "at", "buffer Pool Manager")

		bufferPool.freeFrames = bufferPool.freeFrames[1:]

		slog.Info(fmt.Sprintf("free frame list => %v", bufferPool.freeFrames), "function", "fetchPage", "at", "buffer Pool Manager")
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

	copy(frame.data, data)
	frame.pinCount = 1
	frame.pageId = pageId
	frame.dirty = false

	bufferPool.pageTable[pageId] = newFrameId

	return frame, nil

}

// deletePage is used to deallocate a page which contains data that is no longer useful.
// DO NOT call deletePage directly, as it is not thread-safe.
// always call the DeletePage function of the write guard corresponding to a page, to safely delete it.
func (bufferPool *SimpleBufferPoolManager) deletePage(pageId uint64) (bool, error) {

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

	bufferPool.frameAllocationMutex.Lock()
	// 5. Add frameId to freeFrames.
	bufferPool.freeFrames = append(bufferPool.freeFrames, frameId)
	bufferPool.frameAllocationMutex.Unlock()

	// 6. Delete page table entry.
	delete(bufferPool.pageTable, pageId)

	// 7. Deallocate page in file.
	bufferPool.disk.deallocatePage(pageId)

	// 8. Reset dirty, version, pageId fields of the frame
	frame.pageId = 0
	frame.pinCount = 0

	return true, nil
}

// unpinPage is used to decrement the pin count of a page.
func (bufferPool *SimpleBufferPoolManager) unpinPage(pageId uint64) bool {

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

	if err := bufferPool.releaseAllFrameBuffers(); err != nil {
		return err
	}

	if err := bufferPool.disk.close(); err != nil {
		return err
	}

	return nil

}

func (bufferPool *SimpleBufferPoolManager) releaseAllFrameBuffers() error {

	for _, frame := range bufferPool.frames {
		if err := releaseFrameBuffer(frame.data); err != nil {
			return err
		}
	}
	return nil
}

func releaseFrameBuffer(buffer []byte) error {

	return unix.Munlock(buffer)
}

func createFrameBuffer(size int) ([]byte, error) {

	data := AllocateAlignedBuffer()

	if len(data) < size {
		return nil, fmt.Errorf("buffer size is less than requested size")
	}

	if err := unix.Mlock(data); err != nil {
		return nil, err
	}
	return data, nil
}
