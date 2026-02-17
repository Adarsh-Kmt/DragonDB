package bplustree

import (
	bpm "github.com/Adarsh-Kmt/DragonDB/bufferpoolmanager"
	codec "github.com/Adarsh-Kmt/DragonDB/pagecodec"
)

type ReadCursor struct {
	headerCodec codec.HeaderCodec
	guard       *bpm.ReadGuard
}

type WriteCursor struct {
	headerCodec codec.HeaderCodec
	guard       *bpm.WriteGuard
}

func NewWriteCursor(wg *bpm.WriteGuard) *WriteCursor {
	return &WriteCursor{
		headerCodec: codec.DefaultHeaderCodec(),
		guard:       wg,
	}
}

func NewReadCursor(rg *bpm.ReadGuard) *ReadCursor {
	return &ReadCursor{
		headerCodec: codec.DefaultHeaderCodec(),
		guard:       rg,
	}
}
func (cursor *ReadCursor) GetCurrentNodeReadGuard() *bpm.ReadGuard {

	return cursor.guard
}

func (cursor *ReadCursor) SetCurrentNodeReadGuard(guard *bpm.ReadGuard) {

	cursor.guard = guard
}

func (cursor *ReadCursor) IsLeafNode() bool {

	return cursor.headerCodec.IsLeafNode(cursor.guard.GetPageData())

}

func (cursor *WriteCursor) GetCurrentNodeWriteGuard() *bpm.WriteGuard {

	return cursor.guard
}

func (cursor *WriteCursor) SetCurrentNodeWriteGuard(guard *bpm.WriteGuard) {

	cursor.guard = guard
}

func (cursor *WriteCursor) IsLeafNode() bool {

	return cursor.headerCodec.IsLeafNode(cursor.guard.GetPageData())

}
