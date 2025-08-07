package data_structure_layer

import (
	"bytes"
	"fmt"
	"log/slog"
	"sync"

	bpm "github.com/Adarsh-Kmt/DragonDB/buffer_pool_manager"
	codec "github.com/Adarsh-Kmt/DragonDB/page_codec"
)

type DataStructureLayer interface {
	Get(key []byte) ([]byte, error)
	Insert(key []byte, value []byte) error
	Delete(key []byte) error
	Close() error
}

type BTree struct {
	rootNodePageIdMutex *sync.RWMutex
	metadata            *codec.MetaData
	bufferPoolManager   bpm.BufferPoolManager
}

func NewBTree(bufferPoolManager bpm.BufferPoolManager, metadata *codec.MetaData) *BTree {

	return &BTree{
		rootNodePageIdMutex: &sync.RWMutex{},
		metadata:            metadata,
		bufferPoolManager:   bufferPoolManager,
	}
}

func (btree *BTree) fetchRootNodeReadGuard() (*bpm.ReadGuard, error) {

	btree.rootNodePageIdMutex.RLock()
	defer btree.rootNodePageIdMutex.RUnlock()

	if btree.metadata.RootNodePageId == 0 {
		slog.Info("Root node not found, tree is empty", "function", "fetchRootNodeReadGuard", "at", "btree")
		return nil, fmt.Errorf("root node does not exist")
	} else {
		return btree.bufferPoolManager.NewReadGuard(btree.metadata.RootNodePageId)
	}
}

func (btree *BTree) fetchRootNodeWriteGuard() (*bpm.WriteGuard, error) {

	btree.rootNodePageIdMutex.RLock()
	if btree.metadata.RootNodePageId != uint64(0) {
		slog.Info("Root node already exists, proceeding with insert", "root_node_page_ID", btree.metadata.RootNodePageId, "function", "fetchRootNodeWriteGuard", "at", "btree")
		rootNodeWriteGuard, err := btree.bufferPoolManager.NewWriteGuard(btree.metadata.RootNodePageId)
		btree.rootNodePageIdMutex.RUnlock()
		return rootNodeWriteGuard, err

	}

	btree.rootNodePageIdMutex.RUnlock()

	btree.rootNodePageIdMutex.Lock()
	defer btree.rootNodePageIdMutex.Unlock()

	if btree.metadata.RootNodePageId != uint64(0) {

		return btree.bufferPoolManager.NewWriteGuard(btree.metadata.RootNodePageId)
	}

	// create a new root node.
	slog.Info("Creating new root node for BTree", "function", "fetchRootNodeWriteGuard", "at", "btree")
	rootNodePageId, err := btree.bufferPoolManager.NewPage()
	if err != nil {

		slog.Error("Failed to create new root node page", "error", err.Error(), "function", "fetchRootNodeWriteGuard", "at", "btree")
		return nil, err
	}

	slog.Info("New root node created", "page_ID", rootNodePageId, "function", "fetchRootNodeWriteGuard", "at", "btree")

	rootNodePageGuard, err := btree.bufferPoolManager.NewWriteGuard(rootNodePageId)

	if err != nil {
		btree.bufferPoolManager.CleanupPage(rootNodePageId)
		return nil, err
	}
	btree.metadata.RootNodePageId = rootNodePageId
	return rootNodePageGuard, nil
}
func (btree *BTree) Get(key []byte) ([]byte, error) {
	fmt.Println()
	slog.Info("Starting Get operation", "key", string(key), "function", "Get", "at", "btree")

	slog.Info("Creating read guard for root node", "root_node_page_ID", btree.metadata.RootNodePageId, "function", "Get", "at", "btree")
	rootNodeGuard, err := btree.fetchRootNodeReadGuard()

	if err != nil {
		slog.Error("Failed to create read guard for root node", "error", err.Error(), "function", "Get", "at", "btree")
		return nil, err
	}

	slog.Info("Starting read traversal", "function", "Get", "at", "btree")
	return btree.readTraversal(key, rootNodeGuard)
}

func (btree *BTree) readTraversal(key []byte, guard *bpm.ReadGuard) ([]byte, error) {
	fmt.Println()
	slog.Info("read traversal underway...", "key", string(key), "page ID", guard.GetPageId(), "function", "readTraversal", "at", "btree")

	defer guard.Done()
	value, nextPageId, found := guard.FindElement(key)

	slog.Info("Element search result", "key", string(key), "found", found, "next_page_ID", nextPageId, "is_leaf_node", guard.IsLeafNode(), "function", "readTraversal", "at", "btree")

	if !found {
		if guard.IsLeafNode() {
			slog.Info("Key not found in leaf node", "key", string(key), "function", "readTraversal", "at", "btree")
			return nil, fmt.Errorf("key not found")
		}
	}

	if found {
		slog.Info("Key found, returning value", "key", string(key), "value_length", len(value), "function", "readTraversal", "at", "btree")
		return value, nil
	}

	slog.Info("Traversing to child node", "next_page_ID", nextPageId, "function", "readTraversal", "at", "btree")
	childNodeGuard, err := btree.bufferPoolManager.NewReadGuard(nextPageId)

	if err != nil {
		slog.Error("Failed to create read guard for child node", "next_page_ID", nextPageId, "error", err.Error(), "function", "readTraversal", "at", "btree")
		return nil, err
	}

	return btree.readTraversal(key, childNodeGuard)
}

func (btree *BTree) Insert(key []byte, value []byte) error {

	fmt.Println()
	slog.Info("Starting Insert operation", "key", string(key), "value", string(value), "function", "Insert", "at", "btree")

	rootNodeGuard, err := btree.fetchRootNodeWriteGuard()

	if err != nil {
		slog.Error("Failed to create root node guard", "error", err.Error(), "function", "Insert", "at", "btree")
		return err
	}

	extraKey, extraValue, leftChildNodePageId, rightChildNodePageId, err := btree.writeTraversal(key, value, rootNodeGuard)

	if err != nil {
		slog.Error("Error during write traversal", "error", err.Error(), "function", "Insert", "at", "btree")
		return err
	}
	// new root node required.
	if extraKey != nil {
		slog.Info("Creating new root node due to split", "extra_key", string(extraKey), "left_child_page_ID", leftChildNodePageId, "right_child_page_ID", rightChildNodePageId, "function", "Insert", "at", "btree")

		newRootPageId, err := btree.bufferPoolManager.NewPage()

		if err != nil {
			slog.Error("Failed to create new root node page", "error", err.Error(), "function", "Insert", "at", "btree")
			return err
		}

		newRootGuard, err := btree.bufferPoolManager.NewWriteGuard(newRootPageId)
		if err != nil {
			btree.bufferPoolManager.CleanupPage(newRootPageId)
			slog.Error("Failed to create new root guard", "error", err.Error(), "function", "Insert", "at", "btree")
			return err
		}
		defer newRootGuard.Done()

		newRootGuard.InsertElement(extraKey, extraValue, leftChildNodePageId, rightChildNodePageId)
		newRootGuard.SetDirtyFlag()

		btree.rootNodePageIdMutex.Lock()
		btree.metadata.RootNodePageId = newRootPageId
		btree.rootNodePageIdMutex.Unlock()

		slog.Info("New root node set", "new_root_page_ID", btree.metadata.RootNodePageId, "function", "Insert", "at", "btree")

	}

	return nil
}

func (btree *BTree) writeTraversal(key []byte, value []byte, guard *bpm.WriteGuard) (extraKey []byte, extraValue []byte, leftChildNodePageId uint64, rightChildNodePageId uint64, err error) {

	fmt.Println()
	slog.Info("write traversal underway...", "key", key, "page_ID", guard.GetPageId(), "is_leaf_node", guard.IsLeafNode(), "function", "writeTraversal", "at", "btree")
	defer guard.Done()

	_, nextPageId, found := guard.FindElement(key)

	slog.Info("searching for key in current node...", "key", string(key), "found", found, "next_page_ID", nextPageId, "function", "writeTraversal", "at", "btree")
	if found {

		ok := guard.SetValue(key, value)

		if ok {
			guard.SetDirtyFlag()
			return nil, nil, 0, 0, nil
		}

		rightNodePageId, err := btree.bufferPoolManager.NewPage()

		if err != nil {
			slog.Error("Failed to create new root node page", "error", err.Error(), "function", "writeTraversal", "at", "btree")
			return nil, nil, 0, 0, err
		}
		rightNodeGuard, err := btree.bufferPoolManager.NewWriteGuard(rightNodePageId)
		slog.Info("current leaf node too full, split required", "key", string(key), "page_ID", guard.GetPageId(), "function", "writeTraversal", "at", "btree")
		slog.Info("Creating new right node guard", "right_node_page_ID", rightNodePageId, "function", "writeTraversal", "at", "btree")

		if err != nil {
			btree.bufferPoolManager.CleanupPage(rightNodePageId)
			return nil, nil, 0, 0, err
		}
		defer rightNodeGuard.Done()

		extraKey, extraValue = guard.Split(rightNodeGuard)
		slog.Info("Splitting current node", "key", string(key), " left_node_page_ID", guard.GetPageId(), "right_node_page_ID", rightNodePageId, "function", "writeTraversal", "at", "btree")

		guard.SetDirtyFlag()
		rightNodeGuard.SetDirtyFlag()

		result := bytes.Compare(key, extraKey)

		if result == 0 {
			slog.Info("separator key = target key", "key", string(key), "function", "writeTraversal", "at", "btree")
			extraValue = value
		} else if result < 0 {

			ok := guard.SetValue(key, value)

			if !ok {
				return nil, nil, 0, 0, fmt.Errorf("failed to set value for extra key")
			}
			slog.Info("Setting value for key in left node", "key", string(key), "value", string(value), "function", "writeTraversal", "at", "btree")

		} else {
			ok := rightNodeGuard.SetValue(key, value)

			if !ok {
				return nil, nil, 0, 0, fmt.Errorf("failed to set value for extra key in right node")
			}
			slog.Info("Setting value for key in right node", "key", string(key), "value", string(value), "function", "writeTraversal", "at", "btree")

		}

		return extraKey, extraValue, guard.GetPageId(), rightNodeGuard.GetPageId(), nil
		// set page dirty flag.

	} else {

		if guard.IsLeafNode() || nextPageId == 0 {
			slog.Info("Element not found, inserting into leaf node", "key", string(key), "function", "writeTraversal", "at", "btree")
			ok := guard.InsertElement(key, value, 0, 0)

			if ok {
				guard.SetDirtyFlag()
				return nil, nil, 0, 0, nil
			}

			rightNodePageId, err := btree.bufferPoolManager.NewPage()

			if err != nil {

				slog.Error("Failed to create new right node page", "error", err.Error(), "function", "writeTraversal", "at", "btree")
				return nil, nil, 0, 0, err
			}

			rightNodeGuard, err := btree.bufferPoolManager.NewWriteGuard(rightNodePageId)

			if err != nil {
				btree.bufferPoolManager.CleanupPage(rightNodePageId)
				return nil, nil, 0, 0, err
			}
			slog.Info("Creating new right node guard", "right_node_page_ID", rightNodePageId, "function", "writeTraversal", "at", "btree")

			defer rightNodeGuard.Done()
			extraKey, extraValue = guard.Split(rightNodeGuard)

			rightNodeGuard.SetDirtyFlag()
			guard.SetDirtyFlag()

			result := bytes.Compare(key, extraKey)

			if result < 0 {

				ok := guard.InsertElement(key, value, 0, 0)

				if !ok {
					return nil, nil, 0, 0, fmt.Errorf("failed to set value for extra key")
				}
			} else {
				ok := rightNodeGuard.InsertElement(key, value, 0, 0)

				if !ok {
					return nil, nil, 0, 0, fmt.Errorf("failed to set value for extra key in right node")
				}
			}

			return extraKey, extraValue, guard.GetPageId(), rightNodeGuard.GetPageId(), nil

		} else {
			slog.Info("Element not found, traversing to child node", "next_page_ID", nextPageId, "function", "writeTraversal", "at", "btree")
			childNodeGuard, err := btree.bufferPoolManager.NewWriteGuard(nextPageId)

			if err != nil {

				return nil, nil, 0, 0, err
			}

			extraKey, extraValue, leftChildNodePageId, rightChildNodePageId, err = btree.writeTraversal(key, value, childNodeGuard)

			if err != nil {
				return nil, nil, 0, 0, err
			}

			if extraKey != nil {

				// handle split
				// if extra item causes split, put this <extraKey, extraValue> in the current node, and get a new extrakey, extra value for splitting.

				ok := guard.InsertElement(extraKey, extraValue, leftChildNodePageId, rightChildNodePageId)

				if ok {
					guard.SetDirtyFlag()
					return nil, nil, 0, 0, nil
				}

				rightNodePageId, err := btree.bufferPoolManager.NewPage()
				if err != nil {
					slog.Error("Failed to create new right node page", "error", err.Error(), "function", "writeTraversal", "at", "btree")
					return nil, nil, 0, 0, err
				}
				rightNodeGuard, err := btree.bufferPoolManager.NewWriteGuard(rightNodePageId)

				slog.Info("Creating new right node guard", "right_node_page_ID", rightNodePageId, "function", "writeTraversal", "at", "btree")

				if err != nil {
					btree.bufferPoolManager.CleanupPage(rightNodePageId)
					return nil, nil, 0, 0, err
				}

				defer rightNodeGuard.Done()

				newExtraKey, newExtraValue := guard.Split(rightNodeGuard)
				guard.SetDirtyFlag()
				rightNodeGuard.SetDirtyFlag()

				result := bytes.Compare(extraKey, newExtraKey)

				if result < 0 {

					guard.InsertElement(extraKey, extraValue, leftChildNodePageId, rightChildNodePageId)

				} else if result > 0 {
					rightNodeGuard.InsertElement(extraKey, extraValue, leftChildNodePageId, rightChildNodePageId)
				}

				return newExtraKey, newExtraValue, guard.GetPageId(), rightNodeGuard.GetPageId(), nil
			}
			return nil, nil, 0, 0, nil
		}
	}

}

func (btree *BTree) Delete(key []byte) error {
	return nil
}

func (btree *BTree) Close() error {
	return btree.bufferPoolManager.Close()
}
