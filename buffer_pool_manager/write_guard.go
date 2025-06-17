package buffer_pool_manager

import (
	codec "github.com/Adarsh-Kmt/DragonDB/page_codec"
)

// WriteGuard is used to provide exclusive write access to a page stored in a frame in the buffer pool manager.
type WriteGuard struct {

	// active is used to prevent users from using a write guard once it's Done/DeletePage function has been called.
	active     bool
	page       *Frame
	codec      codec.SlottedPageCodec
	bufferPool BufferPoolManager
}

// NewWriteGuard returns an active write guard.
// All write guards corresponding to a page share a RW lock.
// Each page in the buffer pool manager is associated with a version, which is incremented each time it is updated.
func (bufferPool *SimpleBufferPoolManager) NewWriteGuard(pageId PageID) (*WriteGuard, error) {

	page, err := bufferPool.fetchPage(pageId)

	if err != nil {
		return nil, err
	}

	page.mutex.Lock()

	btreeNode := new(BTreeNode)
	btreeNode.deserialize(page.data)

	guard := &WriteGuard{
		active:     true,
		page:       page,
		bufferPool: bufferPool,
		codec:      codec.DefaultSlottedPageCodec(),
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

func (guard *WriteGuard) InsertElement(key []byte, value []byte) bool {

	if !guard.active {
		return false
	}

	return guard.codec.InsertElement(guard.page.data, key, value)
}

func (guard *WriteGuard) FindElement(key []byte) (value []byte, nextPageId uint32, found bool) {

	if !guard.active {
		return nil, 0, false
	}

	return guard.codec.FindElement(guard.page.data, key)
}

func (guard *WriteGuard) DeleteElement(key []byte) bool {

	if !guard.active {
		return false
	}

	return guard.codec.DeleteElement(guard.page.data, key)
}
