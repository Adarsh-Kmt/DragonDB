package buffer_pool_manager

import (
	"log/slog"
)

// ReadGuard is used to provide shared read access to a page stored in a frame in the buffer pool manager.
type ReadGuard struct {
	active     bool
	page       *Frame
	codec      codec.SlottedPageCodec
	bufferPool BufferPoolManager
}

// NewReadGuard returns an active read guard.
// All read guards corresponding to a page share a RW lock.
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

	return guard, nil

}

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

func (guard *ReadGuard) FindElement(key []byte) (value []byte, nextPageId uint64, found bool) {

	if !guard.active {
		return nil, 0, false
	}

	return guard.codec.FindElement(guard.page.data, key)
}

func (guard *ReadGuard) IsLeafNode() bool {

	if !guard.active {
		return false
	}

	return guard.codec.IsLeafNode(guard.page.data)
}
