package main

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Adarsh-Kmt/DragonDB/buffer_pool_manager"
	"github.com/Adarsh-Kmt/DragonDB/data_structure_layer"
	codec "github.com/Adarsh-Kmt/DragonDB/page_codec"
)

type StorageEngine struct {
	currBTreeId uint64

	openBTreesMutex *sync.Mutex
	openBTrees      map[uint64]*data_structure_layer.BTree
	metadata        *codec.MetaData

	bufferPoolManager buffer_pool_manager.BufferPoolManager
	// WAL dependency

}

func NewStorageEngine() (engine *StorageEngine, isNewDatabase bool, err error) {

	cache := buffer_pool_manager.NewLRUReplacer()
	disk, metadata, isNewDatabase, err := buffer_pool_manager.NewDirectIODiskManager("dragon.db")

	if err != nil {
		return nil, false, err
	}

	bufferPoolManager, err := buffer_pool_manager.NewSimpleBufferPoolManager(5, 4096, cache, disk)

	if err != nil {
		return nil, false, err
	}

	return &StorageEngine{
		currBTreeId: metadata.CurrBTreeId,

		openBTreesMutex: &sync.Mutex{},
		openBTrees:      make(map[uint64]*data_structure_layer.BTree),

		metadata:          metadata,
		bufferPoolManager: bufferPoolManager,
	}, isNewDatabase, err

}
func (engine *StorageEngine) NewBTree() (BTreeId uint64) {

	BTreeId = atomic.AddUint64(&engine.currBTreeId, 1)
	return BTreeId
}

func (engine *StorageEngine) OpenBTree(BTreeId uint64) *data_structure_layer.BTree {

	engine.openBTreesMutex.Lock()
	defer engine.openBTreesMutex.Unlock()

	btree, exists := engine.openBTrees[BTreeId]

	if exists {
		return btree
	}
	btree = data_structure_layer.NewBTree(BTreeId, engine.bufferPoolManager, engine.metadata)
	engine.openBTrees[BTreeId] = btree
	return btree
}

func (engine *StorageEngine) CloseBTree(BTreeId uint64) error {

	engine.openBTreesMutex.Lock()
	defer engine.openBTreesMutex.Unlock()

	btree, exists := engine.openBTrees[BTreeId]

	if !exists {
		return fmt.Errorf("can't close B-Tree twice")
	}
	btree.Close()
	delete(engine.openBTrees, BTreeId)

	return nil
}
func (engine *StorageEngine) Close() error {
	engine.metadata.CurrBTreeId = engine.currBTreeId
	for _, btree := range engine.openBTrees {
		btree.Close()
	}
	return engine.bufferPoolManager.Close()
}
