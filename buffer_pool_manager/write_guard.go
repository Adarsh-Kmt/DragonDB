package buffer_pool_manager

import "errors"

type WriteGuard struct {
	active     bool
	Page       *Frame
	bufferPool BufferPoolManager
}

func (bufferPool *SimpleBufferPoolManager) NewWriteGuard(pageId PageID) (*WriteGuard, error) {

	page, err := bufferPool.fetchPage(pageId)

	if err != nil {
		return nil, err
	}

	versionCopy := page.version

	guard := &WriteGuard{
		active:     true,
		Page:       page,
		bufferPool: bufferPool,
	}

	guard.Page.mutex.Lock()

	if versionCopy != page.version {
		return nil, errors.New("version mismatch error")
	}

	guard.Page.dirty = true

	return guard, nil
}

func (guard *WriteGuard) UpdateVersion() bool {

	if !guard.active {
		return false
	}

	guard.Page.version++

	return true
}

func (guard *WriteGuard) GetData() ([]byte, bool) {

	if !guard.active {
		return nil, false
	}

	return guard.Page.data, true
}

func (guard *WriteGuard) DeletePage() bool {

	if !guard.active {
		return false
	}

	ok, _ := guard.bufferPool.deletePage(guard.Page.pageId)

	if ok {
		return guard.Done()
	}
	return false
}

func (guard *WriteGuard) Done() bool {

	if !guard.active {
		return false
	}
	guard.bufferPool.unpinPage(guard.Page.pageId)

	guard.Page.mutex.Unlock()

	guard.Page = nil
	guard.bufferPool = nil

	return true

}
