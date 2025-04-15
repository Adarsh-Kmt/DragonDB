package buffer_pool_manager

import "errors"

// ReadGuard is used to provide shared read access to a page stored in a frame in the buffer pool manager.
type ReadGuard struct {
	active     bool
	Page       *Frame
	bufferPool BufferPoolManager
}

// NewReadGuard returns an active read guard.
// All read guards corresponding to a page share a RW lock.
// Each page in the buffer pool manager is associated with a version, which is incremented each time it is updated.
// Before acquiring the lock, a copy of the version is recorded.
// Once the lock has been acquired:
// 1. if the copy is equal to the current version of the page, then it wasn't modified before the lock was acquired, and the logical correctness of the page data is maintained.
// 2. if the copy is not equal to the current version of the page, then the page was modified before the lock could be acquired, and its contents cannot be trusted anymore.
func (bufferPool SimpleBufferPoolManager) NewReadGuard(pageId PageID) (*ReadGuard, error) {

	page, err := bufferPool.fetchPage(pageId)

	if err != nil {
		return nil, err
	}

	versionCopy := page.version

	guard := &ReadGuard{
		active:     true,
		Page:       page,
		bufferPool: bufferPool,
	}

	guard.Page.mutex.RLock()

	if page.version != versionCopy {
		return nil, errors.New("version mismatch error")
	}

	return guard, nil

}

// GetData is used to return the page data, provided it is active.
func (guard *ReadGuard) GetData() ([]byte, bool) {

	if !guard.active {
		return nil, false
	}

	return guard.Page.data, true
}

// Done is used to decrease the pin count of the page, and ensure the exclusive lock is released.
// A guard becomes inactive and cannot be reused if this function returns true.
func (guard *ReadGuard) Done() bool {

	if !guard.active {
		return false
	}
	_ = guard.bufferPool.unpinPage(guard.Page.pageId)

	guard.Page.mutex.RUnlock()

	guard.Page = nil
	guard.bufferPool = nil

	return true
}
