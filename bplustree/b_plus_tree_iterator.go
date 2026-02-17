package bplustree

import (
	"fmt"

	bpm "github.com/Adarsh-Kmt/DragonDB/bufferpoolmanager"
)

type BPlusTreeIterator struct {
	cursor            *IterativeCursor
	bufferPoolManager bpm.BufferPoolManager
}

func NewBPlusIterator(bptree *BPlusTree) (*BPlusTreeIterator, error) {

	readGuard, err := bptree.bufferPoolManager.NewReadGuard(bptree.firstLeafNodePageId)

	if err != nil {

		return nil, err
	}

	return &BPlusTreeIterator{
		cursor:            newIterativeCursor(readGuard),
		bufferPoolManager: bptree.bufferPoolManager,
	}, nil
}

func (i *BPlusTreeIterator) Next() (ok bool, err error) {

	nextSlotId := i.cursor.nextSlot()

	if nextSlotId == -1 {
		nextLeafNodePageId := i.cursor.NextLeafNodePageId()

		if nextLeafNodePageId == 0 {
			return false, fmt.Errorf("end of iterator")
		}
		i.cursor.readGuard.Done()

		readGuard, err := i.bufferPoolManager.NewReadGuard(nextLeafNodePageId)

		if err != nil {
			return false, err
		}
		i.cursor.readGuard = readGuard
	}
	return true, nil
}

func (i *BPlusTreeIterator) GetValue() []byte {

	return i.cursor.CurrentValue()
}

func (i *BPlusTreeIterator) Close() {

	i.cursor.readGuard.Done()
	i.bufferPoolManager = nil
}
