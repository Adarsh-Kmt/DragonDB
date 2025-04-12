package buffer_pool_manager

import (
	"container/list"
)

type Replacer interface {
	victim() FrameID
	insert(frameId FrameID)
	remove(frameId FrameID)
	size() int
}

type LRUReplacer struct {
	list     *list.List
	frameMap map[FrameID]*list.Element
}

func NewLRUReplacer() *LRUReplacer {

	return &LRUReplacer{list: list.New(), frameMap: make(map[FrameID]*list.Element)}
}
func (replacer *LRUReplacer) victim() FrameID {

	frameElement := replacer.list.Back()
	frameId := FrameID(replacer.list.Remove(frameElement).(int))

	delete(replacer.frameMap, frameId)
	return frameId
}

func (replacer *LRUReplacer) insert(frameId FrameID) {

	frameElement := replacer.list.PushFront(frameId)
	replacer.frameMap[frameId] = frameElement
}

func (replacer *LRUReplacer) remove(frameId FrameID) {

	frameElement := replacer.frameMap[frameId]
	replacer.list.Remove(frameElement)
	delete(replacer.frameMap, frameId)
}

func (replacer *LRUReplacer) size() int {
	return len(replacer.frameMap)
}
