package main

import (
	"fmt"
	"sync"
	"sync/atomic"

	bplustree "github.com/Adarsh-Kmt/DragonDB/bplustree"
	bpm "github.com/Adarsh-Kmt/DragonDB/bufferpoolmanager"
	codec "github.com/Adarsh-Kmt/DragonDB/pagecodec"
)

type StorageEngine struct {
	currBPlusTreeId uint64

	openBPlusTreesMutex *sync.Mutex
	openBPlusTrees      map[uint64]*bplustree.BPlusTree
	metadata            *codec.MetaData

	bufferPoolManager bpm.BufferPoolManager
	// WAL dependency

}

func NewStorageEngine() (engine *StorageEngine, isNewDatabase bool, err error) {

	cache := bpm.NewLRUReplacer()
	disk, metadata, isNewDatabase, err := bpm.NewDirectIODiskManager("dragon.db")

	if err != nil {
		return nil, false, err
	}

	bufferPoolManager, err := bpm.NewSimpleBufferPoolManager(5, 4096, cache, disk)

	if err != nil {
		return nil, false, err
	}

	return &StorageEngine{
		currBPlusTreeId: metadata.CurrBPlusTreeId,

		openBPlusTreesMutex: &sync.Mutex{},
		openBPlusTrees:      make(map[uint64]*bplustree.BPlusTree),

		metadata:          metadata,
		bufferPoolManager: bufferPoolManager,
	}, isNewDatabase, err

}
func (engine *StorageEngine) NewBPlusTree() (BPlusTreeId uint64) {

	BPlusTreeId = atomic.AddUint64(&engine.currBPlusTreeId, 1)
	return BPlusTreeId
}

func (engine *StorageEngine) OpenBPlusTree(BPlusTreeId uint64) (*bplustree.BPlusTree, error) {

	engine.openBPlusTreesMutex.Lock()
	defer engine.openBPlusTreesMutex.Unlock()

	btree, exists := engine.openBPlusTrees[BPlusTreeId]

	if exists {
		return btree, nil
	}

	return nil, fmt.Errorf("BPlusTree doesnt exist")
}

func (engine *StorageEngine) CloseBPlusTree(BPlusTreeId uint64) error {

	engine.openBPlusTreesMutex.Lock()
	defer engine.openBPlusTreesMutex.Unlock()

	btree, exists := engine.openBPlusTrees[BPlusTreeId]

	if !exists {
		return fmt.Errorf("can't close B-Tree twice")
	}
	btree.Close()
	delete(engine.openBPlusTrees, BPlusTreeId)

	return nil
}
func (engine *StorageEngine) Close() error {
	engine.metadata.CurrBPlusTreeId = engine.currBPlusTreeId
	for _, btree := range engine.openBPlusTrees {
		btree.Close()
	}
	return engine.bufferPoolManager.Close()
}

func (engine *StorageEngine) NewBPlusTreeIterator(BPlusTreeId uint64) (*bplustree.BPlusTreeIterator, error) {

	engine.openBPlusTreesMutex.Lock()
	defer engine.openBPlusTreesMutex.Unlock()

	BPlusTree, ok := engine.openBPlusTrees[BPlusTreeId]

	if !ok {

		BPlusTree, err := engine.OpenBPlusTree(BPlusTreeId)

		if err != nil {
			return nil, err
		}

		engine.openBPlusTrees[BPlusTreeId] = BPlusTree
	}

	return bplustree.NewBPlusIterator(BPlusTree), nil
}
