package buffer_pool_manager

import (
	"log/slog"

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

// GetPageId returns the page ID of the page corresponding to the read guard.
func (guard *WriteGuard) GetPageId() uint64 {

	if !guard.active {
		return 0
	}

	return guard.page.pageId
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

// ------------------------
// Page Content Functions

// InsertElement calls the equivalent InsertElement function of the page codec,
// which inserts an element in the B-Tree node.
func (guard *WriteGuard) InsertElement(key []byte, value []byte, leftChildNodePageId uint64, rightChildNodePageId uint64) bool {

	if !guard.active {
		return false
	}

	return guard.codec.InsertElement(guard.page.data, key, value, leftChildNodePageId, rightChildNodePageId)
}

// FindElement calls the equivalent FindElement function of the page codec,
// which checks if an <key, value> pair exists in a B-Tree node
// or returns the page ID of the child node that must be checked next.
func (guard *WriteGuard) FindElement(key []byte) (value []byte, nextPageId uint64, found bool) {

	if !guard.active {
		return nil, 0, false
	}

	return guard.codec.FindElement(guard.page.data, key)
}

// DeleteElement calls the equivalent DeletElement function of the codec
// which deletes value corresponding to a key in the B-Tree node.
func (guard *WriteGuard) DeleteElement(key []byte) bool {

	if !guard.active {
		return false
	}

	return guard.codec.DeleteElement(guard.page.data, key)
}

// SetValue calls the equivalent SetValue function of the codec, used to set the value of a key int he B-Tre Node.
func (guard *WriteGuard) SetValue(key []byte, value []byte) bool {

	if !guard.active {
		return false
	}

	return guard.codec.SetValue(guard.page.data, key, value)
}

// Split calls the equivalent split function of the codec, used to split a B-Tree node.
func (guard *WriteGuard) Split(rightNodeGuard *WriteGuard) (extraKey []byte, extraValue []byte) {

	if !guard.active {
		return nil, nil
	}

	return guard.codec.Split(guard.page.data, rightNodeGuard.page.data)
}

// IsLeafNode returns true of the node is a leaf node.
func (guard *WriteGuard) IsLeafNode() bool {

	if !guard.active {
		return false
	}

	return guard.codec.IsLeafNode(guard.page.data)
}
