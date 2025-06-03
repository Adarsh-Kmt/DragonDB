package pagecodec

import "encoding/binary"

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

type Header struct {
	isLeafNode     bool
	numSlots       uint16
	garbageSize    uint16
	freeSpaceBegin uint16
	freeSpaceEnd   uint16
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

func (codec *SlottedPageCodec) decodePageHeader(page []byte) Header {

	h := Header{}
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
func (codec *SlottedPageCodec) setNoteType(page []byte, isLeafNode bool) {
	if isLeafNode {
		page[codec.headerConfig.nodeTypeOffset] = codec.headerConfig.leafNodeType
	} else {
		page[codec.headerConfig.nodeTypeOffset] = codec.headerConfig.internalNodeType
	}
}

func (codec *SlottedPageCodec) setNumSlots(page []byte, numSlots int) {

	binary.LittleEndian.PutUint16(page[codec.headerConfig.numSlotsOffset:], uint16(numSlots))
}

func (codec *SlottedPageCodec) setGarbageSize(page []byte, garbageSize uint16) {

	binary.LittleEndian.PutUint16(page[codec.headerConfig.garbageSizeOffset:], garbageSize)
}

func (codec *SlottedPageCodec) setFreeSpaceBegin(page []byte, freeSpaceBegin int) {

	binary.LittleEndian.PutUint16(page[codec.headerConfig.freeSpaceBeginOffset:], uint16(freeSpaceBegin))
}

func (codec *SlottedPageCodec) setFreeSpaceEnd(page []byte, freeSpaceEnd int) {

	binary.LittleEndian.PutUint16(page[codec.headerConfig.freeSpaceEndOffset:], uint16(freeSpaceEnd))
}
