package pagecodec

import (
	"encoding/binary"
	"hash/crc32"
)

type Header struct {
	crc uint32

	// set isleafNode = true if node type field = byte(1), else false
	isLeafNode     bool
	numSlots       uint16
	freeSpaceBegin uint16
	freeSpaceEnd   uint16
	garbageSize    uint16
}

type HeaderConfig struct {

	// header field offsets
	crcOffset            int
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

func defaultHeaderConfig() HeaderConfig {

	return HeaderConfig{
		crcOffset:            0,
		nodeTypeOffset:       4,
		numSlotsOffset:       6,
		freeSpaceBeginOffset: 8,
		freeSpaceEndOffset:   10,
		garbageSizeOffset:    12,

		headerSize:       16,
		leafNodeType:     byte(1),
		internalNodeType: byte(0),
	}
}

// decodePageHeader takes a slice of bytes representing a slotted page header, and returns a deserialized header object
func (codec *SlottedPageCodec) decodePageHeader(headerBytes []byte) Header {

	h := Header{}
	h.crc = binary.LittleEndian.Uint32(headerBytes[codec.headerConfig.crcOffset:])

	if headerBytes[codec.headerConfig.nodeTypeOffset] == codec.headerConfig.leafNodeType {
		h.isLeafNode = true
	} else {
		h.isLeafNode = false
	}

	h.numSlots = binary.LittleEndian.Uint16(headerBytes[codec.headerConfig.numSlotsOffset:])
	h.freeSpaceBegin = binary.LittleEndian.Uint16(headerBytes[codec.headerConfig.freeSpaceBeginOffset:])
	h.freeSpaceEnd = binary.LittleEndian.Uint16(headerBytes[codec.headerConfig.freeSpaceEndOffset:])
	h.garbageSize = binary.LittleEndian.Uint16(headerBytes[codec.headerConfig.garbageSizeOffset:])

	return h

}

// setCRC is used to set the value of the CRC field in the header
func (codec *SlottedPageCodec) setCRC(headerBytes []byte, crc uint32) {

	binary.LittleEndian.PutUint32(headerBytes[codec.headerConfig.crcOffset:], crc)
}

// setNodeType is used to set the value of the node type field in the header
func (codec *SlottedPageCodec) SetNodeType(headerBytes []byte, isLeafNode bool) {

	if isLeafNode {
		headerBytes[codec.headerConfig.nodeTypeOffset] = codec.headerConfig.leafNodeType
	} else {
		headerBytes[codec.headerConfig.nodeTypeOffset] = codec.headerConfig.internalNodeType
	}
}

// setNumSlots is used to set the value of the number of slots field in the header
func (codec *SlottedPageCodec) setNumSlots(headerBytes []byte, numSlots int) {

	binary.LittleEndian.PutUint16(headerBytes[codec.headerConfig.numSlotsOffset:], uint16(numSlots))
}

// setGarbageSize is used to set the value of the garbage size field in the header
func (codec *SlottedPageCodec) setGarbageSize(headerBytes []byte, garbageSize uint16) {

	binary.LittleEndian.PutUint16(headerBytes[codec.headerConfig.garbageSizeOffset:], garbageSize)
}

// setFreeSpaceBegin is used to set the value of the free space begin pointer field in the header
func (codec *SlottedPageCodec) setFreeSpaceBegin(headerBytes []byte, freeSpaceBegin uint16) {

	binary.LittleEndian.PutUint16(headerBytes[codec.headerConfig.freeSpaceBeginOffset:], freeSpaceBegin)
}

// setFreeSpaceEnd is used to set the value of the free space end pointer field in the header
func (codec *SlottedPageCodec) setFreeSpaceEnd(headerBytes []byte, freeSpaceEnd uint16) {

	binary.LittleEndian.PutUint16(headerBytes[codec.headerConfig.freeSpaceEndOffset:], freeSpaceEnd)
}

func generateCRC(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func CheckCRC(data []byte, crc uint32) bool {

	return crc32.ChecksumIEEE(data) == crc
}

func (codec *SlottedPageCodec) updateCRC(page []byte) {

	data := page[4:]
	header := page[:codec.headerConfig.headerSize]

	codec.setCRC(header, generateCRC(data))
}
