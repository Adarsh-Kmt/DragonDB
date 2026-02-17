package bufferpoolmanager

import (
	"log/slog"
)

// WriteGuard is used to provide exclusive write access to a page stored in a frame in the buffer pool manager.
type WriteGuard struct {

	// active is used to prevent users from using a write guard once it's Done/DeletePage function has been called.
	active     bool
	page       *Frame
	bufferPool BufferPoolManager
}

// NewWriteGuard returns an active write guard.
// All guards corresponding to a page share a RW lock.
func (bufferPool *SimpleBufferPoolManager) NewWriteGuard(pageId uint64) (*WriteGuard, error) {

	page, err := bufferPool.fetchPage(pageId)

	if err != nil {
		slog.Error("Failed to fetch page for write guard", "pageId", pageId, "error", err.Error())
		return nil, err
	}

	page.mutex.Lock()

	guard := &WriteGuard{
		active:     true,
		page:       page,
		bufferPool: bufferPool,
	}

	return guard, nil
}

// DeletePage is used to call the delete function of the buffer pool manager in a thread-safe manner.
// A guard becomes inactive and cannot be reused if this function returns true.
func (guard *WriteGuard) DeletePage() bool {

	if !guard.active {
		return false
	}

	ok, _ := guard.bufferPool.deletePage(guard.page.pageId)

	if ok {
		guard.active = false
		guard.page.mutex.Unlock()

		guard.page = nil
		guard.bufferPool = nil
	}
	return false
}

// GetPageId returns the page ID of the page corresponding to the read guard.
func (guard *WriteGuard) GetPageId() uint64 {

	if !guard.active {
		return 0
	}

	return guard.page.pageId
}

func (guard *WriteGuard) GetPageData() []byte {

	if !guard.active {
		return nil
	}
	return guard.page.data
}

func (guard *WriteGuard) IsActive() bool {
	return guard.active
}

// SetDirtyFlag is used to set the dirty flag of the frame in the buffer pool manager
// where the page is stored/
func (guard *WriteGuard) SetDirtyFlag() bool {

	if !guard.active {
		return false
	}

	guard.page.dirty = true

	return true
}

// Done is used to decrease the pin count of the page, and ensure the exclusive lock is released.
// A guard becomes inactive and cannot be reused if this function returns true.
func (guard *WriteGuard) Done() bool {

	if !guard.active {
		return false
	}
	guard.bufferPool.unpinPage(guard.page.pageId)

	guard.page.mutex.Unlock()

	guard.page = nil
	guard.bufferPool = nil
	guard.active = false

	return true

}
