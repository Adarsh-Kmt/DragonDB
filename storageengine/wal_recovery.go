package storageengine

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"

	bplustree "github.com/Adarsh-Kmt/DragonDB/bplustree"
	bpm "github.com/Adarsh-Kmt/DragonDB/bufferpoolmanager"
	lucario "github.com/Adarsh-Kmt/Lucario"
)

type RedoFunc func(record lucario.WALRecord) error

func (engine *StorageEngine) RedoCreatePageOperation(record lucario.WALRecord) error {

	payload := lucario.DecodeCreatePagePayload(record.Payload)
	slog.Info("Redo: CreatePageOperation", "pageId", payload.PageId, "allocationSource", payload.AllocationSource, "pageType", payload.PageType, "lsn", record.LSN)

	if payload.AllocationSource == byte(bpm.FILE_EXPANSION_ALLOCATION) {

		if engine.metadata.LSN < record.LSN {

			if err := engine.bufferPoolManager.EnsurePageExists(payload.PageId); err != nil {
				return err
			}
		}

	} else if payload.AllocationSource == byte(bpm.FREELIST_ALLOCATION) {

		if engine.metadata.LSN < record.LSN {

			length := len(engine.metadata.DeallocatedPageIdList)
			for ind, id := range engine.metadata.DeallocatedPageIdList {

				if id == payload.PageId {
					engine.metadata.DeallocatedPageIdList[ind] = engine.metadata.DeallocatedPageIdList[length-1]
					engine.metadata.DeallocatedPageIdList = engine.metadata.DeallocatedPageIdList[:length-1]
					break
				}
			}
			engine.metadata.LSN = record.LSN
		}
	}

	if payload.PageType == bplustree.LEAF_NODE_TYPE {

		leafNodeWriteGuard, err := engine.bufferPoolManager.NewWriteGuard(payload.PageId)

		if err != nil {
			return err
		}

		defer leafNodeWriteGuard.Done()

		leafNodeWriter := bplustree.NewLeafNodeWriter(leafNodeWriteGuard)

		leafNodeWriter.SetNodeType()

	} else {

		internalNodeWriteGuard, err := engine.bufferPoolManager.NewWriteGuard(payload.PageId)

		if err != nil {
			return err
		}

		defer internalNodeWriteGuard.Done()

		internalNodeWriter := bplustree.NewInternalNodeWriter(internalNodeWriteGuard)

		internalNodeWriter.SetNodeType()

	}

	return nil
}

func (engine *StorageEngine) RedoUpdateRootNodePageId(record lucario.WALRecord) error {

	if engine.metadata.LSN < record.LSN {
		payload := lucario.DecodeUpdateRootNodePageIdPayload(record.Payload)
		slog.Info("Redo: UpdateRootNodePageId", "bplusTreeId", payload.BPlusTreeId, "rootNodePageId", payload.RootNodePageId, "lsn", record.LSN)

		engine.metadata.RootPages[payload.BPlusTreeId] = payload.RootNodePageId
		engine.metadata.LSN = record.LSN
	}

	return nil
}

func (engine *StorageEngine) RedoInsertInternalNodeEntry(record lucario.WALRecord) error {

	payload := lucario.DecodeInsertInternalNodePayload(record.Payload)
	slog.Info("Redo: InsertInternalNodeEntry", "pageId", payload.PageId, "key", string(payload.Key), "leftChildPageId", payload.LeftChildNodePageId, "rightChildPageId", payload.RightChildNodePageId, "lsn", record.LSN)

	insertNodeWriteGuard, err := engine.bufferPoolManager.NewWriteGuard(payload.PageId)

	if err != nil {
		return err
	}

	defer insertNodeWriteGuard.Done()

	insertNodeWriter := bplustree.NewInternalNodeWriter(insertNodeWriteGuard)

	if insertNodeWriter.GetLSN() < record.LSN {
		insertNodeWriter.InsertKey(payload.Key, payload.LeftChildNodePageId, payload.RightChildNodePageId)
		insertNodeWriter.SetLSN(record.LSN)
	}

	return nil
}
func (engine *StorageEngine) RedoUpdateFirstLeafNodePageId(record lucario.WALRecord) error {

	if engine.metadata.LSN < record.LSN {
		payload := lucario.DecodeUpdateFirstLeafNodePageIdPayload(record.Payload)
		slog.Info("Redo: UpdateFirstLeafNodePageId", "bplusTreeId", payload.BPlusTreeId, "firstLeafNodePageId", payload.FirstLeafNodePageId, "lsn", record.LSN)

		engine.metadata.FirstLeafNodePages[payload.BPlusTreeId] = payload.FirstLeafNodePageId
		engine.metadata.LSN = record.LSN
	}

	return nil
}
func (engine *StorageEngine) RedoSplitLeafNode(record lucario.WALRecord) error {

	payload := lucario.DecodeSplitLeafNodePayload(record.Payload)
	slog.Info("Redo: SplitLeafNode", "leftPageId", payload.LeftLeafNodePageId, "rightPageId", payload.RightLeafNodePageId, "separatorKeyIndex", payload.SeparatorKeyIndex, "insertKey", string(payload.InsertKey), "nextLeafNodePageId", payload.NextLeafNodePageId, "lsn", record.LSN)

	fmt.Println("payload.ElementsLength =", payload.ElementsLength)
	fmt.Println("len(payload.Elements) =", len(payload.Elements))
	leftLeafNodeWriteGuard, err := engine.bufferPoolManager.NewWriteGuard(payload.LeftLeafNodePageId)

	if err != nil {
		return err
	}
	defer leftLeafNodeWriteGuard.Done()

	leftLeafNodeWriter := bplustree.NewLeafNodeWriter(leftLeafNodeWriteGuard)

	rightLeafNodeWriteGuard, err := engine.bufferPoolManager.NewWriteGuard(payload.RightLeafNodePageId)

	if err != nil {
		return err
	}
	defer rightLeafNodeWriteGuard.Done()

	rightLeafNodeWriter := bplustree.NewLeafNodeWriter(rightLeafNodeWriteGuard)

	slots, elements := bplustree.GetAllLeafSlotsAndElements(payload.Elements)

	slog.Info("Split recovery details",
		"totalElements", len(elements),
		"separatorIndex", payload.SeparatorKeyIndex,
		"leftElementsCount", int(payload.SeparatorKeyIndex)+1,
		"rightElementsCount", len(elements)-int(payload.SeparatorKeyIndex))

	extraKey := elements[payload.SeparatorKeyIndex].Key

	if rightLeafNodeWriter.GetLSN() < record.LSN {

		rightLeafNodeWriter.PutAllElements(slots[payload.SeparatorKeyIndex:], elements[payload.SeparatorKeyIndex:])
		rightLeafNodeWriter.SetNextLeafNodePageId(payload.NextLeafNodePageId)

		if bytes.Compare(payload.InsertKey, extraKey) >= 0 {
			rightLeafNodeWriter.InsertKeyValue(payload.InsertKey, payload.InsertValue)
		}
		rightLeafNodeWriter.SetLSN(record.LSN)
		rightLeafNodeWriter.PrintElements()
	}

	if leftLeafNodeWriter.GetLSN() < record.LSN {

		leftLeafNodeWriter.PutAllElements(slots[:payload.SeparatorKeyIndex], elements[:payload.SeparatorKeyIndex])
		leftLeafNodeWriter.SetNextLeafNodePageId(rightLeafNodeWriter.GetPageId())

		if bytes.Compare(payload.InsertKey, extraKey) < 0 {
			leftLeafNodeWriter.InsertKeyValue(payload.InsertKey, payload.InsertValue)
		}
		leftLeafNodeWriter.SetLSN(record.LSN)
		leftLeafNodeWriter.PrintElements()

	}

	return nil
}

func (engine *StorageEngine) RedoSplitInternalNode(record lucario.WALRecord) error {

	payload := lucario.DecodeSplitInternalNodePayload(record.Payload)
	slog.Info("Redo: SplitInternalNode", "leftPageId", payload.LeftInternalNodePageId, "rightPageId", payload.RightInternalNodePageId, "separatorKeyIndex", payload.SeparatorKeyIndex, "insertKey", string(payload.InsertKey), "lsn", record.LSN)

	leftInternalNodeWriteGuard, err := engine.bufferPoolManager.NewWriteGuard(payload.LeftInternalNodePageId)

	if err != nil {
		return err
	}

	defer leftInternalNodeWriteGuard.Done()

	leftInternalNodeWriter := bplustree.NewInternalNodeWriter(leftInternalNodeWriteGuard)

	slots, elements := bplustree.GetAllInternalSlotsAndElements(payload.Elements)
	extraKey := elements[payload.SeparatorKeyIndex].Key

	if leftInternalNodeWriter.GetLSN() < record.LSN {

		leftInternalNodeWriter.PutAllElements(slots[:payload.SeparatorKeyIndex], elements[:payload.SeparatorKeyIndex])

		if bytes.Compare(payload.InsertKey, extraKey) < 0 {
			leftInternalNodeWriter.InsertKey(payload.InsertKey, payload.InsertLeftNodePageId, payload.InsertRightNodePageId)
		}
		leftInternalNodeWriter.SetLSN(record.LSN)

	}
	rightInternalNodeWriteGuard, err := engine.bufferPoolManager.NewWriteGuard(payload.RightInternalNodePageId)

	if err != nil {
		return err
	}

	defer rightInternalNodeWriteGuard.Done()

	rightInternalNodeWriter := bplustree.NewInternalNodeWriter(rightInternalNodeWriteGuard)

	if rightInternalNodeWriter.GetLSN() < record.LSN {

		rightInternalNodeWriter.PutAllElements(slots[payload.SeparatorKeyIndex+1:], elements[payload.SeparatorKeyIndex+1:])

		if bytes.Compare(payload.InsertKey, extraKey) >= 0 {
			rightInternalNodeWriter.InsertKey(payload.InsertKey, payload.InsertLeftNodePageId, payload.InsertRightNodePageId)
		}
		rightInternalNodeWriter.SetLSN(record.LSN)
	}

	return nil
}

func (engine *StorageEngine) RedoInsertLeafNodeEntry(record lucario.WALRecord) error {

	payload := lucario.DecodeInsertLeafNodeEntryPayload(record.Payload)
	slog.Info("Redo: InsertLeafNodeEntry", "pageId", payload.PageId, "key", string(payload.Key), "valueLength", len(payload.Value), "lsn", record.LSN)

	leafNodeWriteGuard, err := engine.bufferPoolManager.NewWriteGuard(payload.PageId)

	if err != nil {
		return err
	}

	defer leafNodeWriteGuard.Done()

	leafNodeWriter := bplustree.NewLeafNodeWriter(leafNodeWriteGuard)

	if leafNodeWriter.GetLSN() < record.LSN {
		leafNodeWriter.InsertKeyValue(payload.Key, payload.Value)
		leafNodeWriter.SetLSN(record.LSN)
	}

	return nil
}

func (engine *StorageEngine) RedoUpdateLeafNodeEntry(record lucario.WALRecord) error {

	payload := lucario.DecodeUpdateLeafNodeEntryPayload(record.Payload)
	slog.Info("Redo: UpdateLeafNodeEntry", "pageId", payload.PageId, "key", string(payload.Key), "valueLength", len(payload.Value), "lsn", record.LSN)

	leafNodeWriteGuard, err := engine.bufferPoolManager.NewWriteGuard(payload.PageId)

	if err != nil {
		return err
	}

	defer leafNodeWriteGuard.Done()

	leafNodeWriter := bplustree.NewLeafNodeWriter(leafNodeWriteGuard)

	if leafNodeWriter.GetLSN() < record.LSN {
		leafNodeWriter.SetValue(payload.Key, payload.Value)
		leafNodeWriter.SetLSN(record.LSN)
	}

	return nil
}

func (engine *StorageEngine) Recover() error {

	iterator, err := engine.wal.NewWALIterator()

	if err != nil {
		return err
	}

	slog.Info("starting recovery...", "currOffset", iterator.CurrOffset, "wal file size", iterator.WalFileSize)

	defer iterator.Close()

	inOperation := false

	recoveryOperations := make([]lucario.WALRecord, 0)

	for iterator.HasNext() {

		record, err := iterator.GetRecord()

		if err != nil {
			return err
		}

		slog.Info("WAL RECORD", "LSN", record.LSN, "Operation", record.Operation)

		switch record.Operation {

		case lucario.BeginOperation:

			inOperation = true

		case lucario.CommitOperation:

			if !inOperation {
				return errors.New("CommitOperation without BeginOperation")
			}

			slog.Info("found BEGIN")

			for _, record := range recoveryOperations {

				redo := engine.redoFuncMapping[record.Operation]

				if err := redo(record); err != nil {
					return err
				}
			}

			slog.Info("found COMMIT")

			inOperation = false
			recoveryOperations = recoveryOperations[:0]

		default:

			recoveryOperations = append(recoveryOperations, record)
		}

	}

	if inOperation {

		slog.Info("ignoring incomplete WAL operation at end of log")
	}
	return engine.bufferPoolManager.FlushAllPages()

}
