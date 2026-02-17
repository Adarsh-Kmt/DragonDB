package data_structure_layer

import (
	bpm "github.com/Adarsh-Kmt/DragonDB/buffer_pool_manager"
	codec "github.com/Adarsh-Kmt/DragonDB/pagecodec"
)

/*

Writers/Readers wrap around guards.
Guards control read/write access to the underlying page data stored in the buffer pool frame.
Writers/Readers operate on the contents of the page based on the slotted page structure associated with internal/leaf nodes.
Readers/Writers only expose specific methods required for performing read/write operations and traversals.

*/

// LeafNodeWriter wraps a read guard that controls shared read access to a page containing a internal node
type InternalNodeReader struct {
	guard *bpm.ReadGuard
	codec codec.InternalNodeCodec
}

func NewInternalNodeReader(rg *bpm.ReadGuard) *InternalNodeReader {
	return &InternalNodeReader{
		guard: rg,
		codec: codec.NewInternalNodeCodec(),
	}
}

// FindNextChildNodePageId returns the page id of the next node in the traversal
func (r *InternalNodeReader) FindNextChildNodePageId(key []byte) (pageId uint64) {

	return r.codec.FindNextChildNodePageId(r.guard.GetPageData(), key)
}

func (r *InternalNodeReader) PrintElements() {

	r.codec.PrintElements(r.guard.GetPageData())
}
