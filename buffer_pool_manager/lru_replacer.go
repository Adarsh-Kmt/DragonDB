package buffer_pool_manager

import (
	"container/list"
)

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
	list     *list.List
	frameMap map[FrameID]*list.Element
}

func NewLRUReplacer() *LRUReplacer {

	return &LRUReplacer{list: list.New(), frameMap: make(map[FrameID]*list.Element)}
}

// returns the frame at the back of the list, which is the least recently used frame.
func (replacer *LRUReplacer) victim() FrameID {

	frameElement := replacer.list.Back()
	frameId := FrameID(replacer.list.Remove(frameElement).(int))

	delete(replacer.frameMap, frameId)
	return frameId
}

// inserts the frame at the front of the list, it becomes the most recently used frame.
func (replacer *LRUReplacer) insert(frameId FrameID) {

	frameElement := replacer.list.PushFront(frameId)
	replacer.frameMap[frameId] = frameElement
}

// removes frame from the list, used once its pin count > 0.
func (replacer *LRUReplacer) remove(frameId FrameID) {

	frameElement := replacer.frameMap[frameId]
	replacer.list.Remove(frameElement)
	delete(replacer.frameMap, frameId)
}

func (replacer *LRUReplacer) size() int {
	return len(replacer.frameMap)
}
