package buffer_pool_manager

import "errors"

type ReadGuard struct {
	active     bool
	Page       *Frame
	bufferPool BufferPoolManager
}

func (bufferPool *SimpleBufferPoolManager) NewReadGuard(pageId PageID) (*ReadGuard, error) {

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

func (guard *ReadGuard) GetData() ([]byte, bool) {

	if !guard.active {
		return nil, false
	}

	return guard.Page.data, true
}

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
