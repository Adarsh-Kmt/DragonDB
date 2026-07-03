package storageengine

import (
	"fmt"
	"sync"
	"sync/atomic"

	bplustree "github.com/Adarsh-Kmt/DragonDB/bplustree"
	bpm "github.com/Adarsh-Kmt/DragonDB/bufferpoolmanager"
	codec "github.com/Adarsh-Kmt/DragonDB/pagecodec"
	lucario "github.com/Adarsh-Kmt/Lucario"
)

type StorageEngine struct {
	currBPlusTreeId uint64

	openBPlusTreesMutex *sync.Mutex
	openBPlusTrees      map[uint64]*bplustree.BPlusTree
	metadata            *codec.MetaData

	wal             *lucario.WAL
	redoFuncMapping map[lucario.Operation]RedoFunc

	bufferPoolManager bpm.BufferPoolManager
	// WAL dependency

}

func NewStorageEngine() (engine *StorageEngine, isNewDatabase bool, err error) {

	engine = &StorageEngine{}

	cache := bpm.NewLRUReplacer()
	disk, metadata, isNewDatabase, err := bpm.NewDirectIODiskManager("dragon.db")

	if err != nil {
		return nil, false, err
	}

	bufferPoolManager, err := bpm.NewSimpleBufferPoolManager(5, 4096, cache, disk)

	if err != nil {
		return nil, false, err
	}

	wal, err := lucario.NewWAL("./lucario.wal")

	if err != nil {
		return nil, false, err
	}

	engine.currBPlusTreeId = metadata.CurrBPlusTreeId

	engine.openBPlusTreesMutex = &sync.Mutex{}
	engine.openBPlusTrees = make(map[uint64]*bplustree.BPlusTree)
	engine.metadata = metadata

	engine.wal = wal

	engine.redoFuncMapping = map[lucario.Operation]RedoFunc{
		lucario.CreatePage:                engine.RedoCreatePageOperation,
		lucario.UpdateRootNodePageId:      engine.RedoUpdateRootNodePageId,
		lucario.UpdateFirstLeafNodePageId: engine.RedoUpdateFirstLeafNodePageId,
		lucario.UpdateLeafNodeEntry:       engine.RedoUpdateLeafNodeEntry,
		lucario.InsertLeafNodeEntry:       engine.RedoInsertLeafNodeEntry,
		lucario.InsertInternalNodeEntry:   engine.RedoInsertInternalNodeEntry,
		lucario.SplitInternalNode:         engine.RedoSplitInternalNode,
		lucario.SplitLeafNode:             engine.RedoSplitLeafNode,
	}

	engine.bufferPoolManager = bufferPoolManager

	return engine, isNewDatabase, err

}
func (engine *StorageEngine) NewBPlusTree() (BPlusTreeId uint64) {

	BPlusTreeId = atomic.AddUint64(&engine.currBPlusTreeId, 1)
	return BPlusTreeId
}

func (engine *StorageEngine) OpenBPlusTree(BPlusTreeId uint64) (btree *bplustree.BPlusTree, exists bool) {

	engine.openBPlusTreesMutex.Lock()
	defer engine.openBPlusTreesMutex.Unlock()

	btree, exists = engine.openBPlusTrees[BPlusTreeId]

	if !exists {

		btree = bplustree.NewBPlusTree(BPlusTreeId, engine.bufferPoolManager, engine.metadata, engine.wal)

		engine.openBPlusTrees[BPlusTreeId] = btree
	}
	return btree, true
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

		BPlusTree, exists := engine.OpenBPlusTree(BPlusTreeId)

		if !exists {
			return nil, fmt.Errorf("B Plus Tree doesnt exist")
		}

		engine.openBPlusTrees[BPlusTreeId] = BPlusTree
	}
	return bplustree.NewBPlusIterator(BPlusTree)
}

func (engine *StorageEngine) GetCurrBPlusTreeId() uint64 {

	return engine.currBPlusTreeId
}
