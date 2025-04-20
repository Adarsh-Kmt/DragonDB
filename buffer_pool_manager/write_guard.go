package buffer_pool_manager

import (
	"errors"
)

// WriteGuard is used to provide exclusive write access to a page stored in a frame in the buffer pool manager.
type WriteGuard struct {

	// active is used to prevent users from using a write guard once it's Done/DeletePage function has been called.
	active     bool
	page       *Frame
	btreeNode  *BTreeNode
	bufferPool BufferPoolManager
}

// NewWriteGuard returns an active write guard.
// All write guards corresponding to a page share a RW lock.
// Each page in the buffer pool manager is associated with a version, which is incremented each time it is updated.
// Before acquiring the lock, a copy of the version is recorded.
// Once the lock has been acquired:
// 1. if the copy is equal to the current version of the page, then it wasn't modified before the lock was acquired, and the logical correctness of the page data is maintained.
// 2. if the copy is not equal to the current version of the page, then the page was modified before the lock could be acquired, and its contents cannot be trusted anymore.
func (bufferPool SimpleBufferPoolManager) NewWriteGuard(pageId PageID) (*WriteGuard, error) {

	page, err := bufferPool.fetchPage(pageId)

	if err != nil {
		return nil, err
	}

	versionCopy := page.version

	page.mutex.Lock()

	if versionCopy != page.version {
		page.mutex.Unlock()
		return nil, errors.New("version mismatch error")
	}

	btreeNode := new(BTreeNode)
	btreeNode.deserialize(page.data)

	guard := &WriteGuard{
		active:     true,
		page:       page,
		bufferPool: bufferPool,
		btreeNode:  btreeNode,
	}

	guard.page.dirty = true

	return guard, nil
}

// UpdateVersion is used to increment the version of a page.
// This is used to signal to the other guards that the page has been modified.
func (guard *WriteGuard) UpdateVersion() bool {

	if !guard.active {
		return false
	}

	guard.page.version++

	return true
}

func (guard *WriteGuard) GetVersion() (int, bool) {

	if !guard.active {
		return -1, false
	}

	return guard.page.version, true
}

// GetData is used to return the BTreeNode corresponding to the page, provided it is active.
func (guard *WriteGuard) GetData() (*BTreeNode, bool) {

	if !guard.active {
		return nil, false
	}
	// if guard.btreeNode != nil {
	// 	return guard.btreeNode, true
	// }

	// guard.btreeNode = new(BTreeNode)
	// guard.btreeNode.deserialize(guard.page.data)

	return guard.btreeNode, true
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

		guard.btreeNode = nil
		guard.page = nil
		guard.bufferPool = nil
	}
	return false
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
