package buffer_pool_manager

import (
	"container/list"
	"sync"
)

// keeps track of unused occupied frames.
type Replacer interface {

	// victim selects a frame to evict based on the replacement policy.
	victim() FrameID

	// insert adds a frame to the replacer, marking it as a candidate for eviction.
	insert(frameId FrameID)

	// remove eliminates a frame from the replacer, typically when the frame is pinned.
	remove(frameId FrameID)

	// size returns the current number of frames managed by the replacer.
	size() int
}

type LRUReplacer struct {

	// synchronizes access to the list.
	mutex *sync.Mutex

	// keeps track of the order in which frames were last accessed.
	list *list.List

	// used to remove frames from the middle of the list.
	frameMap map[FrameID]*list.Element
}

func NewLRUReplacer() *LRUReplacer {

	return &LRUReplacer{
		list:     list.New(),
		frameMap: make(map[FrameID]*list.Element),
		mutex:    &sync.Mutex{},
	}
}

// removes and returns the ID of the frame at the back of the list, which is the least recently accessed frame.
func (replacer *LRUReplacer) victim() FrameID {

	replacer.mutex.Lock()
	defer replacer.mutex.Unlock()

	frameElement := replacer.list.Back()
	frameId := FrameID(replacer.list.Remove(frameElement).(FrameID))

	delete(replacer.frameMap, frameId)
	return frameId
}

// inserts the frame ID at the front of the list, it becomes the most recently accessed frame.
func (replacer *LRUReplacer) insert(frameId FrameID) {

	replacer.mutex.Lock()
	defer replacer.mutex.Unlock()

	frameElement := replacer.list.PushFront(frameId)
	replacer.frameMap[frameId] = frameElement
}

// removes frame from the list once its pin count > 0.
func (replacer *LRUReplacer) remove(frameId FrameID) {

	replacer.mutex.Lock()
	defer replacer.mutex.Unlock()

	frameElement := replacer.frameMap[frameId]
	replacer.list.Remove(frameElement)
	delete(replacer.frameMap, frameId)
}

// returns the number of frames currently managed by the replacer.
func (replacer *LRUReplacer) size() int {

	replacer.mutex.Lock()
	defer replacer.mutex.Unlock()

	return len(replacer.frameMap)
}
