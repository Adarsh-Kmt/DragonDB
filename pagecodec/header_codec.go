package pagecodec

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log/slog"
)

type HeaderCodec struct {
	config HeaderConfig
}
type Header struct {
	crc uint32

	// set isleafNode = true if node type field = byte(1), else false
	isLeafNode         bool
	isPageFilled       bool
	numSlots           uint16
	freeSpaceBegin     uint16
	freeSpaceEnd       uint16
	garbageSize        uint16
	nextLeafNodePageId uint64
}

type HeaderConfig struct {

	// header field offsets
	crcOffset                int
	nodeTypeOffset           int
	isPageFilledOffset       int
	numSlotsOffset           int
	garbageSizeOffset        int
	freeSpaceBeginOffset     int
	freeSpaceEndOffset       int
	nextLeafNodePageIdOffset int

	// constants
	headerSize       int
	pageFilledType   uint8
	pageEmptyType    uint8
	leafNodeType     uint8
	internalNodeType uint8
}

func defaultHeaderConfig() HeaderConfig {

	h := HeaderConfig{
		crcOffset:                0,
		nodeTypeOffset:           4,
		isPageFilledOffset:       5,
		numSlotsOffset:           6,
		freeSpaceBeginOffset:     8,
		freeSpaceEndOffset:       10,
		garbageSizeOffset:        12,
		nextLeafNodePageIdOffset: 16,

		headerSize:       24,
		pageFilledType:   byte(1),
		pageEmptyType:    byte(0),
		leafNodeType:     byte(0),
		internalNodeType: byte(1),
	}

	return h
}

func DefaultHeaderCodec() HeaderCodec {
	return HeaderCodec{
		config: defaultHeaderConfig(),
	}
}

func (codec HeaderCodec) getHeaderSize() int {
	return codec.config.headerSize
}

func (codec HeaderCodec) getLeafNodeType() uint8 {
	return codec.config.leafNodeType
}

func (codec HeaderCodec) getInternalNodeType() uint8 {
	return codec.config.internalNodeType
}

// decodePageHeader takes a slice of bytes representing a slotted page header, and returns a deserialized header object
func (codec HeaderCodec) decodePageHeader(headerBytes []byte) *Header {

	fmt.Println()

	slog.Info("Decoding page header...", "function", "decodePageHeader", "at", "HeaderCodec")
	h := &Header{}

	if headerBytes[codec.config.isPageFilledOffset] == codec.config.pageEmptyType {
		// If the page is empty, return an empty header
		h.numSlots = 0
		h.freeSpaceBegin = uint16(codec.config.headerSize)
		h.freeSpaceEnd = 4096
		h.garbageSize = 0
		h.isLeafNode = true // Default to leaf node type for empty pages
		h.crc = 0

		//slog.Info("Decoded page header", "is leaf node", h.isLeafNode, "number of slots", h.numSlots, "free space begin", h.freeSpaceBegin, "free space end", h.freeSpaceEnd, "garbage size", h.garbageSize, "function", "decodePageHeader", "at", "HeaderCodec") // CRC is zero for empty pages
		return h
	}
	h.crc = binary.LittleEndian.Uint32(headerBytes[codec.config.crcOffset:])

	if headerBytes[codec.config.nodeTypeOffset] == codec.config.leafNodeType {
		h.isLeafNode = true
	} else {
		h.isLeafNode = false
	}

	h.numSlots = binary.LittleEndian.Uint16(headerBytes[codec.config.numSlotsOffset:])
	h.freeSpaceBegin = binary.LittleEndian.Uint16(headerBytes[codec.config.freeSpaceBeginOffset:])
	h.freeSpaceEnd = binary.LittleEndian.Uint16(headerBytes[codec.config.freeSpaceEndOffset:])
	h.garbageSize = binary.LittleEndian.Uint16(headerBytes[codec.config.garbageSizeOffset:])
	h.nextLeafNodePageId = binary.LittleEndian.Uint64(headerBytes[codec.config.nextLeafNodePageIdOffset:])

	slog.Info("Decoded Page Header", "is leaf node", h.isLeafNode, "number of slots", h.numSlots, "free space begin", h.freeSpaceBegin, "free space end", h.freeSpaceEnd, "garbage size", h.garbageSize, "function", "decodePageHeader", "at", "HeaderCodec")
	return h

}

var zeroBlock = make([]byte, 1024)

func isPageEmpty(page []byte) bool {
	fmt.Println()

	//slog.Info("Checking if page is empty...", "function", "isPageEmpty", "at", "HeaderCodec")
	for _, b := range page {
		if b != 0 {
			//slog.Info("Page is not empty", "function", "isPageEmpty", "at", "HeaderCodec")
			return false
		}
	}
	//slog.Info("Page is empty", "function", "isPageEmpty", "at", "HeaderCodec")
	return true
}

// setCRC is used to set the value of the CRC field in the header
func (codec HeaderCodec) setCRC(headerBytes []byte, crc uint32) {
	binary.LittleEndian.PutUint32(headerBytes[codec.config.crcOffset:], crc)
}

// setNodeType is used to set the value of the node type field in the header
func (codec HeaderCodec) SetNodeType(headerBytes []byte, isLeafNode bool) {

	if isLeafNode {
		headerBytes[codec.config.nodeTypeOffset] = codec.config.leafNodeType
	} else {
		headerBytes[codec.config.nodeTypeOffset] = codec.config.internalNodeType
	}
}

func (codec HeaderCodec) SetIsPageFilled(headerBytes []byte, isPageFilled bool) {

	if isPageFilled {
		headerBytes[codec.config.isPageFilledOffset] = codec.config.pageFilledType
	} else {
		headerBytes[codec.config.isPageFilledOffset] = codec.config.pageEmptyType
	}
}

// setNumSlots is used to set the value of the number of slots field in the header
func (codec HeaderCodec) setNumSlots(headerBytes []byte, numSlots int) {

	binary.LittleEndian.PutUint16(headerBytes[codec.config.numSlotsOffset:], uint16(numSlots))
}

// setGarbageSize is used to set the value of the garbage size field in the header
func (codec HeaderCodec) setGarbageSize(headerBytes []byte, garbageSize uint16) {

	binary.LittleEndian.PutUint16(headerBytes[codec.config.garbageSizeOffset:], garbageSize)
}

// setFreeSpaceBegin is used to set the value of the free space begin pointer field in the header
func (codec HeaderCodec) setFreeSpaceBegin(headerBytes []byte, freeSpaceBegin uint16) {

	binary.LittleEndian.PutUint16(headerBytes[codec.config.freeSpaceBeginOffset:], freeSpaceBegin)
}

// setFreeSpaceEnd is used to set the value of the free space end pointer field in the header
func (codec HeaderCodec) setFreeSpaceEnd(headerBytes []byte, freeSpaceEnd uint16) {

	binary.LittleEndian.PutUint16(headerBytes[codec.config.freeSpaceEndOffset:], freeSpaceEnd)
}

func (codec HeaderCodec) setNextLeafNodePageId(headerBytes []byte, nextLeafNodePageId uint64) {

	binary.LittleEndian.PutUint64(headerBytes[codec.config.nextLeafNodePageIdOffset:], nextLeafNodePageId)
}

func generateCRC(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func CheckCRC(data []byte, crc uint32) bool {

	return crc32.ChecksumIEEE(data) == crc
}

func (codec HeaderCodec) updateCRC(page []byte) {

	data := page[4:]
	header := page[:codec.config.headerSize]

	codec.setCRC(header, generateCRC(data))
}

func (codec *HeaderCodec) IsLeafNode(page []byte) bool {

	headerBytes := page[:codec.config.headerSize]
	// slog.Info("header size", "size", codec.config.headerSize)
	// slog.Info(fmt.Sprintf("extracted header %v", headerBytes))
	// slog.Info(fmt.Sprintf("header size %d", len(headerBytes)))
	header := codec.decodePageHeader(headerBytes)

	return header.isLeafNode
}

// isAdequate is used to check whether the page has the required amount of free space or not
func (codec HeaderCodec) isAdequate(page []byte, spaceRequired int) bool {

	header := codec.decodePageHeader(page)

	freeSpace := header.freeSpaceEnd - header.freeSpaceBegin

	slog.Info("Checking if page has enough space...", "requiredSpace", spaceRequired, "freeSpace", freeSpace, "function", "isAdequate", "at", "HeaderCodec")
	return freeSpace >= uint16(spaceRequired)
}

// shoudCompact is used to check whether compaction will free up the required amount of space or not
func (codec HeaderCodec) shouldCompact(page []byte, size int) bool {

	header := codec.decodePageHeader(page)

	return size <= int(header.garbageSize)
}

// getTotalDataRegionSize returns the size of the data region
func (codec HeaderCodec) getTotalDataRegionSize(slots []Slot) uint16 {

	size := uint16(0)

	for _, slot := range slots {
		size += slot.elementSize
	}
	return size
}
