package buffer_pool_manager

import (
	"log/slog"

	codec "github.com/Adarsh-Kmt/DragonDB/page_codec"
)

// ReadGuard is used to provide shared read access to a page stored in a frame in the buffer pool manager.
type ReadGuard struct {
	active     bool
	page       *Frame
	codec      codec.SlottedPageCodec
	bufferPool BufferPoolManager
}

// NewReadGuard returns an active read guard.
// All guards corresponding to a page share a RW lock.
func (bufferPool *SimpleBufferPoolManager) NewReadGuard(pageId uint64) (*ReadGuard, error) {

	page, err := bufferPool.fetchPage(pageId)

	if err != nil {
		slog.Error("Failed to fetch page for read guard", "pageId", pageId, "error", err.Error())
		return nil, err
	}

	page.mutex.RLock()

	guard := &ReadGuard{
		active:     true,
		page:       page,
		bufferPool: bufferPool,
		codec:      codec.DefaultSlottedPageCodec(),
	}

	guard.codec.PrintElements(page.data)
	return guard, nil

}

// GetPageId returns the page ID of the page corresponding to the read guard.
func (guard *ReadGuard) GetPageId() uint64 {

	if !guard.active {
		return 0
	}

	return guard.page.pageId
}

// Done is used to decrease the pin count of the page, and ensure the exclusive lock is released.
// A guard becomes inactive and cannot be reused if this function returns true.
func (guard *ReadGuard) Done() bool {

	if !guard.active {
		return false
	}
	_ = guard.bufferPool.unpinPage(guard.page.pageId)

	guard.page.mutex.RUnlock()

	guard.page = nil
	guard.bufferPool = nil

	return true
}

// FindElement calls the equivalent FindElement function of the page codec,
// which checks if an <key, value> pair exists in a B-Tree node
// or returns the page ID of the child node that must be checked next.
func (guard *ReadGuard) FindElement(key []byte) (value []byte, nextPageId uint64, found bool) {

	if !guard.active {
		return nil, 0, false
	}

	return guard.codec.FindElement(guard.page.data, key)
}

// IsLeafNode returns true of the node is a leaf node.
func (guard *ReadGuard) IsLeafNode() bool {

	if !guard.active {
		return false
	}

	return guard.codec.IsLeafNode(guard.page.data)
}
