package bplustree

import (
	"fmt"
	"log/slog"

	bpm "github.com/Adarsh-Kmt/DragonDB/bufferpoolmanager"
	codec "github.com/Adarsh-Kmt/DragonDB/pagecodec"
)

// LeafNodeWriter wraps a write guard that controls exclusive write access to a page containing a leaf node
type LeafNodeWriter struct {
	guard *bpm.WriteGuard
	codec codec.LeafNodeCodec
}

func NewLeafNodeWriter(wg *bpm.WriteGuard) *LeafNodeWriter {

	return &LeafNodeWriter{
		guard: wg,
		codec: codec.NewLeafNodeCodec(),
	}
}

// GetPageId returns the page ID of the page corresponding to the read guard.
func (w *LeafNodeWriter) GetPageId() uint64 {

	if !w.guard.IsActive() {
		return 0
	}

	return w.guard.GetPageId()
}

// SetNodeType sets the NodeType field in the header of the page to "leaf node"
func (w *LeafNodeWriter) SetNodeType() {

	w.guard.SetDirtyFlag()
	w.codec.SetNodeType(w.guard.GetPageData())
}

// InsertKeyValue inserts a key value element in the B+ Tree leaf node
func (w *LeafNodeWriter) InsertKeyValue(key []byte, value []byte) bool {

	if !w.guard.IsActive() {
		return false
	}
	slog.Info(fmt.Sprintf("inserting key %s into page-id %d", string(key), w.GetPageId()))
	w.guard.SetDirtyFlag()

	return w.codec.InsertElement(w.guard.GetPageData(), key, value)
}

// FindValue searches for and returns value corresponding to key
func (w *LeafNodeWriter) FindValue(key []byte) (value []byte, found bool) {

	if !w.guard.IsActive() {
		return nil, false
	}

	return w.codec.FindValue(w.guard.GetPageData(), key)
}

// DeleteKeyValue deletes key value pair in the B+ tree leaf node
func (w *LeafNodeWriter) DeleteKeyValue(key []byte) bool {

	if !w.guard.IsActive() {
		return false
	}
	w.guard.SetDirtyFlag()

	return w.codec.DeleteElement(w.guard.GetPageData(), key)
}

// SetValue sets a new value for an existing key in the B+ Tree leaf node
func (w *LeafNodeWriter) SetValue(key []byte, value []byte) bool {

	if !w.guard.IsActive() {
		return false
	}
	w.guard.SetDirtyFlag()
	w.codec.SetValue(w.guard.GetPageData(), key, value)
	return true
}

func (w *LeafNodeWriter) SetLSN(LSN uint64) {
	w.guard.SetDirtyFlag()
	w.codec.SetLSN(w.guard.GetPageData(), LSN)
}

func (w *LeafNodeWriter) GetLSN() (LSN uint64) {

	return w.codec.GetLSN(w.guard.GetPageData())
}

func (w *LeafNodeWriter) SetNextLeafNodePageId(nextLeafNodePageId uint64) {
	w.guard.SetDirtyFlag()
	w.codec.SetNextLeafNodePageId(w.guard.GetPageData(), nextLeafNodePageId)
}

func (w *LeafNodeWriter) GetNextLeafNodePageId() (nextLeafNodePageId uint64) {

	return w.codec.GetNextLeafNodePageId(w.guard.GetPageData())
}
func (w *LeafNodeWriter) EncodeAllElements() (elementListLength int, payload []byte) {

	return w.codec.EncodeAllElements(w.guard.GetPageData())
}

func GetAllLeafSlotsAndElements(elementListBytes []byte) ([]codec.Slot, []codec.LeafNodeElement) {

	codec := codec.NewLeafNodeCodec()
	return codec.DecodeAllSlotsAndElements(elementListBytes)
}
func (w *LeafNodeWriter) PutAllElements(slots []codec.Slot, elements []codec.LeafNodeElement) {
	w.guard.SetDirtyFlag()
	w.codec.PutAllSlotsAndElements(w.guard.GetPageData(), slots, elements)
}

func (w *LeafNodeWriter) FindSplitIndex() (separatorIndex int) {

	return w.codec.FindSplitNodeIndex(w.guard.GetPageData())
}

// Split is used to split a B+ Tree leaf node
func (w *LeafNodeWriter) Split(rightLeafNodeWriter *LeafNodeWriter, splitIndex int) (extraKey []byte) {

	if !w.guard.IsActive() {
		return nil
	}

	w.guard.SetDirtyFlag()
	rightLeafNodeWriter.guard.SetDirtyFlag()

	slog.Info(fmt.Sprintf("splitting node %d", w.GetPageId()))

	return w.codec.SplitNode(w.guard.GetPageData(), rightLeafNodeWriter.guard.GetPageData(), rightLeafNodeWriter.GetPageId(), splitIndex)

}

func (w *LeafNodeWriter) HasEnoughSpaceToUpdateValue(key []byte, oldValue []byte, newValue []byte) bool {

	if !w.guard.IsActive() {
		return false
	}

	return w.codec.HasEnoughSpaceToUpdateValue(w.guard.GetPageData(), key, oldValue, newValue)

}

func (w *LeafNodeWriter) HasEnoughSpaceToInsertElement(key []byte, value []byte) bool {

	if !w.guard.IsActive() {
		return false
	}

	return w.codec.HasEnoughSpaceToInsertElement(w.guard.GetPageData(), key, value)

}

func (w *LeafNodeWriter) PrintElements() {

	w.codec.PrintElements(w.guard.GetPageData())
}
