package bplustree

import (
	"bytes"
	"fmt"
	"log/slog"
	"sync"

	bpm "github.com/Adarsh-Kmt/DragonDB/bufferpoolmanager"
	codec "github.com/Adarsh-Kmt/DragonDB/pagecodec"
	lucario "github.com/Adarsh-Kmt/Lucario"
)

const (
	LEAF_NODE_TYPE     = byte(0)
	INTERNAL_NODE_TYPE = byte(1)
)

type BPlusTree struct {
	BPlusTreeId         uint64
	rootNodePageId      uint64
	firstLeafNodePageId uint64
	//rootNodePageIdMutex *sync.RWMutex

	wal *lucario.WAL

	bPlusTreeMutex    *sync.RWMutex
	metadata          *codec.MetaData
	bufferPoolManager bpm.BufferPoolManager
}

func NewBPlusTree(BPlusTreeId uint64, bufferPoolManager bpm.BufferPoolManager, metadata *codec.MetaData, wal *lucario.WAL) *BPlusTree {

	bptree := &BPlusTree{
		BPlusTreeId:       BPlusTreeId,
		rootNodePageId:    metadata.RootPages[BPlusTreeId],
		bPlusTreeMutex:    &sync.RWMutex{},
		metadata:          metadata,
		bufferPoolManager: bufferPoolManager,
		wal:               wal,
	}
	return bptree
}

func (bptree *BPlusTree) Get(key []byte) ([]byte, error) {

	bptree.bPlusTreeMutex.RLock()
	defer bptree.bPlusTreeMutex.RUnlock()

	fmt.Println()
	slog.Info("Starting Get operation", "key", string(key), "function", "Get", "at", "btree")

	slog.Info("Creating read guard for root node", "root_node_page_ID", bptree.rootNodePageId, "function", "Get", "at", "btree")
	//rootNodeGuard, err := bptree.fetchRootNodeReadGuard()

	rootNodeGuard, err := bptree.bufferPoolManager.NewReadGuard(bptree.rootNodePageId)

	if err != nil {
		slog.Error("Failed to create read guard for root node", "error", err.Error(), "function", "Get", "at", "btree")
		return nil, err
	}

	defer rootNodeGuard.Done()

	readCursor := NewReadCursor(rootNodeGuard)

	slog.Info("Starting read traversal", "function", "Get", "at", "btree")
	return bptree.readTraversal(key, readCursor)
}

func (bptree *BPlusTree) readTraversal(key []byte, cursor *ReadCursor) ([]byte, error) {
	fmt.Println()
	slog.Info("read traversal underway...", "key", string(key), "page ID", cursor.GetCurrentNodeReadGuard().GetPageId(), "function", "readTraversal", "at", "btree")

	if cursor.IsLeafNode() {

		leafNodeReader := NewLeafNodeReader(cursor.GetCurrentNodeReadGuard())
		leafNodeReader.PrintElements()
		value, ok := leafNodeReader.FindValue(key)

		if !ok {
			slog.Info("Key not found in leaf node", "key", string(key), "function", "readTraversal", "at", "btree")
			return nil, fmt.Errorf("key not found")
		}

		slog.Info("Key found, returning value", "key", string(key), "value_length", len(value), "function", "readTraversal", "at", "btree")
		return value, nil
	}

	internalNodeReader := NewInternalNodeReader(cursor.GetCurrentNodeReadGuard())
	internalNodeReader.PrintElements()
	childNodePageId := internalNodeReader.FindNextChildNodePageId(key)

	slog.Info("Element search result", "key", string(key), "next_page_ID", childNodePageId, "is_leaf_node", false, "function", "readTraversal", "at", "btree")
	childNodeReadGuard, err := bptree.bufferPoolManager.NewReadGuard(childNodePageId)

	if err != nil {
		slog.Error("Failed to create read guard for child node", "next_page_ID", childNodePageId, "error", err.Error(), "function", "readTraversal", "at", "btree")
		return nil, err
	}

	defer childNodeReadGuard.Done()
	cursor.SetCurrentNodeReadGuard(childNodeReadGuard)

	slog.Info("Traversing to child node", "next_page_ID", childNodePageId, "function", "readTraversal", "at", "bptree")

	return bptree.readTraversal(key, cursor)
}

func (bptree *BPlusTree) Insert(key []byte, value []byte) error {
	// slog.Info("before insert")
	// bptree.bufferPoolManager.PrintAllPages()
	// print := func() {
	// 	slog.Info("after insert pages")
	// 	bptree.bufferPoolManager.PrintAllPages()
	// }
	// defer print()
	bptree.bPlusTreeMutex.Lock()
	defer bptree.bPlusTreeMutex.Unlock()

	var rootNodeGuard *bpm.WriteGuard

	if bptree.rootNodePageId == 0 {

		rootNodePageId, allocationSource, err := bptree.bufferPoolManager.NewPage()
		if err != nil {
			return err
		}

		lsn, err := bptree.wal.LogCreatePageOperation(rootNodePageId, LEAF_NODE_TYPE, byte(allocationSource))

		if err != nil {
			return err
		}

		rootNodeGuard, err = bptree.bufferPoolManager.NewWriteGuard(rootNodePageId)

		if err != nil {
			slog.Error("Failed to create root node guard", "error", err.Error(), "function", "Insert", "at", "bptree")
			return err
		}

		rootNodeWriter := NewLeafNodeWriter(rootNodeGuard)
		rootNodeWriter.SetLSN(lsn)

		lsn, err = bptree.wal.LogUpdateFirstLeafNodePageIdOperation(bptree.BPlusTreeId, rootNodePageId)

		if err != nil {
			return err
		}

		bptree.metadata.LSN = lsn

		lsn, err = bptree.wal.LogUpdateRootNodePageIdOperation(bptree.BPlusTreeId, rootNodePageId)

		if err != nil {
			return err
		}

		bptree.metadata.LSN = lsn

		bptree.firstLeafNodePageId = rootNodePageId

		bptree.rootNodePageId = rootNodePageId

	} else {

		var err error
		rootNodeGuard, err = bptree.bufferPoolManager.NewWriteGuard(bptree.rootNodePageId)

		if err != nil {
			return err
		}
		bptree.rootNodePageId = rootNodePageId
	}

	fmt.Println()
	slog.Info("Starting Insert operation", "key", string(key), "function", "Insert", "at", "bptree")

	rootNodeGuard, err := bptree.bufferPoolManager.NewWriteGuard(bptree.rootNodePageId)

	if err != nil {
		slog.Error("Failed to create root node guard", "error", err.Error(), "function", "Insert", "at", "bptree")
		return err
	}

	defer rootNodeGuard.Done()

	writeCursor := NewWriteCursor(rootNodeGuard)
	_, _, _, err := bptree.writeTraversal(key, value, writeCursor)

	if err != nil {
		slog.Error("Error during write traversal", "error", err.Error(), "function", "Insert", "at", "btree")
		return err
	}

	return nil
}

func (bptree *BPlusTree) HandleLeafRootNodeSplit(oldRootNodeWriter *LeafNodeWriter, key []byte, value []byte) error {

	slog.Info("Creating new root node due to split", "extra_key", string(key), "value", string(value), "function", "Insert", "at", "btree")
	newRootPageId, allocationSource, err := bptree.bufferPoolManager.NewPage()

	if err != nil {
		slog.Error("Failed to create new root node page", "error", err.Error(), "function", "Insert", "at", "btree")
		return err
	}

	newRootGuard, err := bptree.bufferPoolManager.NewWriteGuard(newRootPageId)
	if err != nil {
		bptree.bufferPoolManager.CleanupPage(newRootPageId)
		slog.Error("Failed to create new root guard", "error", err.Error(), "function", "Insert", "at", "btree")
		return err
	}
	defer newRootGuard.Done()

	lsn, err := bptree.wal.LogCreatePageOperation(newRootPageId, INTERNAL_NODE_TYPE, byte(allocationSource))

	if err != nil {
		return err
	}

	newRootNodeWriter := NewInternalNodeWriter(newRootGuard)
	newRootNodeWriter.SetLSN(lsn)
	newRootNodeWriter.SetNodeType()

	extraKey, leftChildNodePageId, rightChildNodePageId, err := bptree.HandleLeafNodeSplit(oldRootNodeWriter, newRootNodeWriter, key, value)

	if err != nil {
		return err
	}

	newRootNodeWriter.InsertKey(extraKey, leftChildNodePageId, rightChildNodePageId)

	lsn, err = bptree.wal.LogUpdateRootNodePageIdOperation(bptree.BPlusTreeId, newRootPageId)

	if err != nil {
		return err
	}
	bptree.metadata.LSN = lsn
	bptree.rootNodePageId = newRootPageId

	return nil

}
func (bptree *BPlusTree) HandleInternalRootNodeSplit(oldRootNodeWriter *InternalNodeWriter, insertKey []byte, insertLeftChildNodePageId uint64, insertRightChildNodePageId uint64) (err error) {

	newRootPageId, allocationSource, err := bptree.bufferPoolManager.NewPage()

	if err != nil {
		slog.Error("Failed to create new root node page", "error", err.Error(), "function", "Insert", "at", "btree")
		return err
	}

	newRootGuard, err := bptree.bufferPoolManager.NewWriteGuard(newRootPageId)
	if err != nil {
		bptree.bufferPoolManager.CleanupPage(newRootPageId)
		slog.Error("Failed to create new root guard", "error", err.Error(), "function", "Insert", "at", "btree")
		return err
	}
	defer newRootGuard.Done()

	lsn, err := bptree.wal.LogCreatePageOperation(newRootPageId, INTERNAL_NODE_TYPE, byte(allocationSource))

	if err != nil {
		return err
	}

	newRootNodeWriter := NewInternalNodeWriter(newRootGuard)

	newRootNodeWriter.SetLSN(lsn)

	extraKey, leftChildNodePageId, rightChildNodePageId, err := bptree.HandleInternalNodeSplit(
		oldRootNodeWriter,
		newRootNodeWriter,
		insertKey,
		insertLeftChildNodePageId,
		insertRightChildNodePageId,
	)

	if err != nil {
		return err
	}

	newRootNodeWriter.InsertKey(extraKey, leftChildNodePageId, rightChildNodePageId)

	lsn, err = bptree.wal.LogUpdateRootNodePageIdOperation(bptree.BPlusTreeId, newRootPageId)

	if err != nil {
		return err
	}
	bptree.metadata.LSN = lsn
	bptree.rootNodePageId = newRootPageId
	slog.Info("New root node set", "new_root_page_ID", bptree.rootNodePageId, "function", "Insert", "at", "bptree")

	return nil

}

func (bptree *BPlusTree) HandleLeafNodeSplit(leftLeafNodeWriter *LeafNodeWriter, parentNodeWriter *InternalNodeWriter, key []byte, value []byte) (extraKey []byte, leftChildNodePageId uint64, rightChildNodePageId uint64, err error) {

	rightChildNodePageId, allocationSource, err := bptree.bufferPoolManager.NewPage()

			if err != nil {
				return nil, 0, 0, err
			}

			writeGuard, err := bptree.bufferPoolManager.NewWriteGuard(rightChildNodePageId)

			if err != nil {

				bptree.bufferPoolManager.CleanupPage(rightChildNodePageId)
				return nil, 0, 0, err
			}

			defer writeGuard.Done()

			rightLeafNodeWriter := NewLeafNodeWriter(writeGuard)

			extraKey := leafNodeWriter.Split(rightLeafNodeWriter)

			if bytes.Compare(key, extraKey) < 0 {
				leafNodeWriter.InsertKeyValue(key, value)
			} else {
				rightLeafNodeWriter.InsertKeyValue(key, value)
			}
			return extraKey, leafNodeWriter.GetPageId(), rightLeafNodeWriter.GetPageId(), nil

		} else {

			ok := leafNodeWriter.InsertKeyValue(key, value)

			if ok {
				return nil, 0, 0, nil
			}

			rightChildNodePageId, err := bptree.bufferPoolManager.NewPage()

			if err != nil {
				return nil, 0, 0, err
			}

			writeGuard, err := bptree.bufferPoolManager.NewWriteGuard(rightChildNodePageId)

			if err != nil {

				bptree.bufferPoolManager.CleanupPage(rightChildNodePageId)
				return nil, 0, 0, err
			}

			defer writeGuard.Done()

			rightLeafNodeWriter := NewLeafNodeWriter(writeGuard)
			rightLeafNodeWriter.SetNodeType()
			extraKey := leafNodeWriter.Split(rightLeafNodeWriter)

			if bytes.Compare(key, extraKey) < 0 {

				leafNodeWriter.InsertKeyValue(key, value)

			} else {

				rightLeafNodeWriter.InsertKeyValue(key, value)

			}

			leafNodeWriter.PrintElements()
			rightLeafNodeWriter.PrintElements()

			return extraKey, leafNodeWriter.GetPageId(), rightLeafNodeWriter.GetPageId(), nil

		}

	}

	internalNodeWriter := NewInternalNodeWriter(cursor.GetCurrentNodeWriteGuard())

	nextChildNodePageId := internalNodeWriter.FindNextChildNodePageId(key)

	childNodeWriteGuard, err := bptree.bufferPoolManager.NewWriteGuard(nextChildNodePageId)

	if err != nil {

		return nil, 0, 0, err
	}

	defer childNodeWriteGuard.Done()

	cursor.SetCurrentNodeWriteGuard(childNodeWriteGuard)

	extraKey, leftChildNodePageId, rightChildNodePageId, err = bptree.writeTraversal(key, value, cursor)

	if err != nil {
		return nil, 0, 0, err
	}

	if extraKey == nil {
		return nil, 0, 0, nil
	}

	internalNodeWriter = NewInternalNodeWriter(currWriteGuard)

	ok := internalNodeWriter.InsertKey(extraKey, leftChildNodePageId, rightChildNodePageId)

	if ok {
		return nil, 0, 0, nil
	}

	rightChildNodePageId, err = bptree.bufferPoolManager.NewPage()

	if err != nil {
		return nil, 0, 0, err
	}

	writeGuard, err := bptree.bufferPoolManager.NewWriteGuard(rightChildNodePageId)

	if err != nil {

		bptree.bufferPoolManager.CleanupPage(rightChildNodePageId)
		return nil, 0, 0, err
	}

	defer writeGuard.Done()

	rightInternalNodeWriter := NewInternalNodeWriter(writeGuard)

	splitKey := internalNodeWriter.Split(rightInternalNodeWriter)

	if bytes.Compare(extraKey, splitKey) < 0 {
		internalNodeWriter.InsertKey(extraKey, leftChildNodePageId, rightChildNodePageId)
	} else {
		rightInternalNodeWriter.InsertKey(extraKey, leftChildNodePageId, rightChildNodePageId)
	}

	return splitKey, internalNodeWriter.GetPageId(), rightInternalNodeWriter.GetPageId(), nil

}

func (bptree *BPlusTree) Delete(key []byte) error {
	return nil
}
func (bptree *BPlusTree) Close() {
	bptree.metadata.RootPages[bptree.BPlusTreeId] = bptree.rootNodePageId
	bptree.metadata.FirstLeafNodePages[bptree.BPlusTreeId] = bptree.firstLeafNodePageId
}
