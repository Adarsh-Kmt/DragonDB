package bplustree

import (
	bpm "github.com/Adarsh-Kmt/DragonDB/bufferpoolmanager"
	codec "github.com/Adarsh-Kmt/DragonDB/pagecodec"
)

type IterativeCursor struct {
	readGuard     *bpm.ReadGuard
	currentSlotId int
	codec         codec.LeafNodeCodec
}

func newIterativeCursor(rg *bpm.ReadGuard) *IterativeCursor {
	return &IterativeCursor{
		readGuard:     rg,
		currentSlotId: 0,
		codec:         codec.NewLeafNodeCodec(),
	}
}
func (i *IterativeCursor) NextLeafNodePageId() uint64 {
	i.currentSlotId = 0
	return i.codec.GetNextLeafNodePageId(i.readGuard.GetPageData())
}

func (i *IterativeCursor) CurrentValue() []byte {

	return i.codec.GetValueCorrespondingToSlot(i.readGuard.GetPageData(), uint16(i.currentSlotId))
}

func (i *IterativeCursor) nextSlot() int {

	numSlots := int(i.codec.GetNumSlots(i.readGuard.GetPageData()))
	currentSlotId := i.currentSlotId
	for currentSlotId < numSlots && !i.codec.IsSlotDeleted(i.readGuard.GetPageData(), currentSlotId) {
		currentSlotId++
	}
	if currentSlotId == numSlots {
		return -1
	}
	return currentSlotId
}
