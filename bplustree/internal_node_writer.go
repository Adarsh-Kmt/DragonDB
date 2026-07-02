package bplustree

import (
	"fmt"
	"log/slog"

	bpm "github.com/Adarsh-Kmt/DragonDB/bufferpoolmanager"
	codec "github.com/Adarsh-Kmt/DragonDB/pagecodec"
)

// InternalNodeWriter wraps a write guard that controls exclusive write access to a page containing a internal node.
type InternalNodeWriter struct {
	guard *bpm.WriteGuard
	codec codec.InternalNodeCodec
}

func NewInternalNodeWriter(wg *bpm.WriteGuard) *InternalNodeWriter {

	return &InternalNodeWriter{
		guard: wg,
		codec: codec.NewInternalNodeCodec(),
	}
}

// GetPageDataId returns the page ID of the page managed by the guard
func (w *InternalNodeWriter) GetPageId() uint64 {

	if !w.guard.IsActive() {
		return 0
	}

	return w.guard.GetPageId()
}

// SetNodeType sets the NodeType field in the header of the page to "internal node"
func (w *InternalNodeWriter) SetNodeType() {

	w.codec.SetNodeType(w.guard.GetPageData())
}

// InsertKey inserts an element (key, left/right child node page ID) in the internal node.
func (w *InternalNodeWriter) InsertKey(key []byte, leftChildNodePageId uint64, rightChildNodePageId uint64) bool {

	if !w.guard.IsActive() {
		return false
	}

	w.guard.SetDirtyFlag()
	slog.Info(fmt.Sprintf("inserting key %s into page-id %d", string(key), w.GetPageId()))
	return w.codec.InsertElement(w.guard.GetPageData(), key, leftChildNodePageId, rightChildNodePageId)
}

// FindNextChildNodePageId returns the page id of the next node in the traversal
func (w *InternalNodeWriter) FindNextChildNodePageId(key []byte) (nextPageId uint64) {

	if !w.guard.IsActive() {
		return 0
	}

	return w.codec.FindNextChildNodePageId(w.guard.GetPageData(), key)
}

// DeleteKey deletes a key from the internal node.
func (w *InternalNodeWriter) DeleteKey(key []byte) bool {

	if !w.guard.IsActive() {
		return false
	}

	w.guard.SetDirtyFlag()
	return w.codec.DeleteElement(w.guard.GetPageData(), key)
}

func (w *InternalNodeWriter) HasEnoughSpaceToInsertElement(key []byte) bool {

	return w.codec.HasEnoughSpaceToInsertElement(w.guard.GetPageData(), key)
}

func (w *InternalNodeWriter) SetLSN(LSN uint64) {
	w.guard.SetDirtyFlag()
	w.codec.SetLSN(w.guard.GetPageData(), LSN)
}

func (w *InternalNodeWriter) GetLSN() (LSN uint64) {

	return w.codec.GetLSN(w.guard.GetPageData())
}

func (w *InternalNodeWriter) EncodeAllElements() (elementListLength int, payload []byte) {

	return w.codec.EncodeAllElements(w.guard.GetPageData())
}

func GetAllInternalSlotsAndElements(elementListBytes []byte) ([]codec.Slot, []codec.InternalNodeElement) {

	codec := codec.NewInternalNodeCodec()
	return codec.DecodeAllSlotsAndElements(elementListBytes)
}
func (w *InternalNodeWriter) PutAllElements(slots []codec.Slot, elements []codec.InternalNodeElement) {
	w.guard.SetDirtyFlag()
	w.codec.PutAllSlotsAndElements(w.guard.GetPageData(), slots, elements)
}

func (w *InternalNodeWriter) FindSplitIndex() int {

	return w.codec.FindSplitNodeIndex(w.guard.GetPageData())
}

// Split is used to split a B+ Tree internal node.
// Some elements remain in the node, others elements are move to the right node passed as a argument.
// function returns the extra key to be sent to the parent node.
func (w *InternalNodeWriter) Split(rightNodeWriter *InternalNodeWriter, splitIndex int) (extraKey []byte) {

	if !w.guard.IsActive() {
		return nil
	}

	w.guard.SetDirtyFlag()
	rightNodeWriter.guard.SetDirtyFlag()

	extraKey = w.codec.SplitNode(w.guard.GetPageData(), rightNodeWriter.guard.GetPageData(), rightNodeWriter.GetPageId(), splitIndex)

	return extraKey
}

func (w *InternalNodeWriter) PrintElements() {

	w.codec.PrintElements(w.guard.GetPageData())
}
