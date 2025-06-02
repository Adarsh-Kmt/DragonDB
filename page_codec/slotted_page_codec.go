package pagecodec

import "encoding/binary"

type SlottedPageCodec struct {
	headerConfig HeaderConfig
	slotConfig   SlotConfig
}

type HeaderConfig struct {

	// header field offsets
	nodeTypeOffset       int
	numSlotsOffset       int
	garbageSizeOffset    int
	freeSpaceBeginOffset int
	freeSpaceEndOffset   int

	// constants
	headerSize       int
	leafNodeType     uint8
	internalNodeType uint8
}

type SlotConfig struct {

	// slot field offsets
	elementPointerOffset int
	elementSizeOffset    int

	// constants
	slotSize                 int
	deletedElementPointerVal uint16
}

type Header struct {
	isLeafNode     bool
	numSlots       uint16
	garbageSize    uint16
	freeSpaceBegin uint16
	freeSpaceEnd   uint16
}
type Element struct {
	Key                  []byte
	Value                []byte
	LeftChildNodePageId  uint32
	RightChildNodePageId uint32
}

func DefaultHeaderConfig() HeaderConfig {

	return HeaderConfig{
		nodeTypeOffset:       0,
		numSlotsOffset:       1,
		garbageSizeOffset:    3,
		freeSpaceBeginOffset: 5,
		freeSpaceEndOffset:   7,

		leafNodeType:     byte(0),
		internalNodeType: byte(1),
	}
}

// header region functions

func (codec *SlottedPageCodec) DecodePageHeader(page []byte) *Header {

	h := &Header{}
	if page[codec.headerConfig.nodeTypeOffset] == codec.headerConfig.leafNodeType {
		h.isLeafNode = true
	} else {
		h.isLeafNode = false
	}

	h.numSlots = binary.LittleEndian.Uint16(page[codec.headerConfig.numSlotsOffset:])
	h.garbageSize = binary.LittleEndian.Uint16(page[codec.headerConfig.garbageSizeOffset:])
	h.freeSpaceBegin = binary.LittleEndian.Uint16(page[codec.headerConfig.freeSpaceBeginOffset:])
	h.freeSpaceEnd = binary.LittleEndian.Uint16(page[codec.headerConfig.freeSpaceEndOffset:])

	return h

}
func (codec *SlottedPageCodec) SetNoteType(page []byte, isLeafNode bool) {
	if isLeafNode {
		page[codec.headerConfig.nodeTypeOffset] = codec.headerConfig.leafNodeType
	} else {
		page[codec.headerConfig.nodeTypeOffset] = codec.headerConfig.internalNodeType
	}
}

func (codec *SlottedPageCodec) SetNumSlots(page []byte, numSlots uint16) {

	binary.LittleEndian.PutUint16(page[codec.headerConfig.numSlotsOffset:], numSlots)
}

func (codec *SlottedPageCodec) SetGarbageSize(page []byte, garbageSize uint16) {

	binary.LittleEndian.PutUint16(page[codec.headerConfig.freeSpaceEndOffset:], garbageSize)
}

func (codec *SlottedPageCodec) SetFreeSpaceBegin(page []byte, freeSpaceBegin uint16) {

	binary.LittleEndian.PutUint16(page[codec.headerConfig.freeSpaceBeginOffset:], freeSpaceBegin)
}

func (codec *SlottedPageCodec) SetfreeSpaceEnd(page []byte, freeSpaceEnd uint16) {

	binary.LittleEndian.PutUint16(page[codec.headerConfig.freeSpaceEndOffset:], freeSpaceEnd)
}

// slot region functions
func (codec *SlottedPageCodec) GetSlot(page []byte, offset int) (elementPointer uint16, elementSize uint16)

func (codec *SlottedPageCodec) GetElement(page []byte, elementOffset uint16) (key []byte, value []byte, leftChildNodePageId uint32, rightChildNodePageId uint32)

func (codec *SlottedPageCodec) InsertElement(page []byte, key []byte, value []byte, leftChildNodePageId uint32, rightChildNodePageId uint32) bool

func (codec *SlottedPageCodec) IsAdequate(page []byte, key, value []byte) bool

func (codec *SlottedPageCodec) Split(page []byte) (node1 []byte, node2 []byte)

func (codec *SlottedPageCodec) Compact(page []byte)
