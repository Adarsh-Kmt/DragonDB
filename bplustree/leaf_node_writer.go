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

	w.codec.SetNodeType(w.guard.GetPageData())
}

// InsertKeyValue inserts a key value element in the B+ Tree leaf node
func (w *LeafNodeWriter) InsertKeyValue(key []byte, value []byte) bool {

	if !w.guard.IsActive() {
		return false
	}
	slog.Info(fmt.Sprintf("inserting key %s value %s into page-id %d", string(key), string(value), w.GetPageId()))
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

	return w.codec.DeleteElement(w.guard.GetPageData(), key)
}

// SetValue sets a new value for an existing key in the B+ Tree leaf node
func (w *LeafNodeWriter) SetValue(key []byte, value []byte) bool {

	if !w.guard.IsActive() {
		return false
	}

	return w.codec.SetValue(w.guard.GetPageData(), key, value)
}

// Split is used to split a B+ Tree leaf node
func (w *LeafNodeWriter) Split(rightLeafNodeWrite *LeafNodeWriter) (extraKey []byte) {

	if !w.guard.IsActive() {
		return nil
	}

	slog.Info(fmt.Sprintf("splitting node %d", w.GetPageId()))
	return w.codec.SplitNode(w.guard.GetPageData(), rightLeafNodeWrite.guard.GetPageData(), rightLeafNodeWrite.GetPageId())
}

func (w *LeafNodeWriter) PrintElements() {

	w.codec.PrintElements(w.guard.GetPageData())
}
