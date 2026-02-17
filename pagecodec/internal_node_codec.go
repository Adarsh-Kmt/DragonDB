package pagecodec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log/slog"
)

type InternalNodeCodec struct {
	headerCodec HeaderCodec
	slotCodec   SlotCodec
}
type InternalNodeElement struct {
	Key                  []byte
	LeftChildNodePageId  uint64
	RightChildNodePageId uint64
}

func NewInternalNodeCodec() InternalNodeCodec {

	return InternalNodeCodec{
		headerCodec: DefaultHeaderCodec(),
		slotCodec:   defaultSlotCodec(),
	}
}

// decodeInternalNodeElement takes a slice of bytes representing a element in the data region, and returns a deserialized element object
func (codec InternalNodeCodec) decodeElement(elementBytes []byte) InternalNodeElement {

	e := InternalNodeElement{}

	pointer := uint16(0)

	// decode key length field
	keyLength := binary.LittleEndian.Uint16(elementBytes[pointer:])
	pointer += 2

	key := make([]byte, keyLength)

	// extract key
	copy(key, elementBytes[pointer:pointer+keyLength])
	e.Key = key

	pointer += keyLength

	// decode left child node page ID
	e.LeftChildNodePageId = binary.LittleEndian.Uint64(elementBytes[pointer:])
	pointer += 8

	// decode right child node page ID
	e.RightChildNodePageId = binary.LittleEndian.Uint64(elementBytes[pointer:])

	return e

}

// encodeSlot takes an element struct and returns an encoded slice of bytes representing this element
func (codec InternalNodeCodec) encodeElement(element InternalNodeElement) []byte {

	fmt.Println()
	//slog.Info("Encoding element...", "function", "encodeInternalNodeElement", "at", "InternalNodeCodec")
	b := make([]byte, 0)

	b = binary.LittleEndian.AppendUint16(b, uint16(len(element.Key)))

	b = append(b, element.Key...)

	b = binary.LittleEndian.AppendUint64(b, element.LeftChildNodePageId)

	b = binary.LittleEndian.AppendUint64(b, element.RightChildNodePageId)

	return b
}

func (codec InternalNodeCodec) SetNodeType(page []byte) {

	codec.headerCodec.SetNodeType(page[:codec.headerCodec.getHeaderSize()], false)
}
func (codec InternalNodeCodec) setRightChildNodePageId(elementBytes []byte, rightChildNodePageId uint64) {

	pointer := 0

	keySize := binary.LittleEndian.Uint16(elementBytes[pointer:])
	pointer += 2

	pointer += int(keySize)

	pointer += 8

	binary.LittleEndian.PutUint64(elementBytes[pointer:], rightChildNodePageId)

}

func (codec InternalNodeCodec) setLeftChildNodePageId(elementBytes []byte, leftChildNodePageId uint64) {

	pointer := 0

	keySize := binary.LittleEndian.Uint16(elementBytes[pointer:])
	pointer += 2

	pointer += int(keySize)

	binary.LittleEndian.PutUint64(elementBytes[pointer:], leftChildNodePageId)

}

func (codec InternalNodeCodec) FindNextChildNodePageId(page []byte, key []byte) (nextChildNodePageId uint64) {

	_, elements := codec.getAllSlotsAndElements(page)

	nextChildNodePageId = 0

	for _, element := range elements {

		result := bytes.Compare(element.Key, key)

		if result == 0 {
			return element.RightChildNodePageId

		} else if result == 1 {
			return element.LeftChildNodePageId
		}

		nextChildNodePageId = element.RightChildNodePageId
	}

	return nextChildNodePageId
}

// InsertElement is used to insert a key value pair in a page
func (codec InternalNodeCodec) InsertElement(page []byte, key []byte, leftChildNodePageId uint64, rightChildNodePageId uint64) bool {

	fmt.Println()
	slog.Info("Inserting element in page...", "key", string(key), "left_child_node_page_ID", leftChildNodePageId, "right_child_node_page_ID", rightChildNodePageId, "function", "InsertElement", "at", "InternalNodeCodec")
	defer codec.headerCodec.updateCRC(page)

	// extract header bytes from page
	headerBytes := page[:codec.headerCodec.getHeaderSize()]
	// slog.Info(fmt.Sprintf("extracted header %v of size %d", headerBytes, codec.headerCodec.getHeaderSize()))
	// slog.Info(fmt.Sprintf("header size %d", len(headerBytes)))
	// decode header
	header := codec.headerCodec.decodePageHeader(headerBytes)

	// calculate space required to store element
	elementSpaceRequired := 2 + len(key) + 8 + 8

	// calculate space required to store new slot
	slotSpaceRequired := codec.slotCodec.getSlotSize()

	//slog.Info("Element space required", "size", elementSpaceRequired, "function", "InsertElement", "at", "InternalNodeCodec")

	// check if free space region has enough space to accomodate new element
	if !codec.headerCodec.isAdequate(page, elementSpaceRequired+slotSpaceRequired) {

		slog.Info("Not enough space in free space region", "function", "InsertElement", "at", "InternalNodeCodec")
		//slog.Info("checking if compaction will help...", "function", "InsertElement", "at", "InternalNodeCodec")
		// if free space is not adequate, check if performing compaction will help
		if !codec.headerCodec.shouldCompact(page, elementSpaceRequired+slotSpaceRequired) {
			//slog.Info("Compaction will not help", "function", "InsertElement", "at", "InternalNodeCodec")
			// if compaction doesnt free up enough space, return false.
			return false
		} else {
			//slog.Info("Compaction will help, initiating compaction....", "function", "InsertElement", "at", "InternalNodeCodec")
			// if compaction frees up enough space to insert new element + new slot, perform compaction
			codec.compact(page)

			// update the header after compaction
			headerBytes = page[:codec.headerCodec.getHeaderSize()]
			header = codec.headerCodec.decodePageHeader(headerBytes)
		}
	}

	// create new element
	newElement := InternalNodeElement{
		Key:                  key,
		LeftChildNodePageId:  leftChildNodePageId,
		RightChildNodePageId: rightChildNodePageId,
	}

	// append new element to end of free space region
	header.freeSpaceEnd = codec.appendElement(page, header.freeSpaceEnd, newElement)
	// create new slot
	newSlot := Slot{
		elementSize:    codec.calculateElementSize(newElement),
		elementPointer: header.freeSpaceEnd,
	}

	header.freeSpaceBegin = codec.InsertSlot(page, newSlot, key, leftChildNodePageId, rightChildNodePageId)
	// update free space end pointer field in header region
	codec.headerCodec.setFreeSpaceEnd(headerBytes, header.freeSpaceEnd)
	// update free space begin pointer field in header region
	codec.headerCodec.setFreeSpaceBegin(headerBytes, header.freeSpaceBegin)
	// update number of slots field in header region
	codec.headerCodec.setNumSlots(headerBytes, int(header.numSlots)+1)
	codec.headerCodec.SetIsPageFilled(headerBytes, true)

	return true
}

// getAllSlotsAndElements returns a list of slots and their corresponding elements in the page. This function skips deleted elements
func (codec InternalNodeCodec) getAllSlotsAndElements(page []byte) ([]Slot, []InternalNodeElement) {

	header := codec.headerCodec.decodePageHeader(page)
	pointer := codec.headerCodec.getHeaderSize()

	slots := make([]Slot, 0)
	elements := make([]InternalNodeElement, 0)

	for range int(header.numSlots) {

		slotBytes := page[pointer : pointer+codec.slotCodec.getSlotSize()]

		slot := codec.slotCodec.decodeSlot(slotBytes)
		pointer += 4

		if !codec.slotCodec.isElementDeleted(slot) {

			elementBytes := page[slot.elementPointer : slot.elementPointer+slot.elementSize]
			element := codec.decodeElement(elementBytes)
			slots = append(slots, slot)
			elements = append(elements, element)
		}
	}

	return slots, elements
}

// putAllSlotsAndElements inserts slots and elements into the page, assuming it to be empty
func (codec InternalNodeCodec) putAllSlotsAndElements(page []byte, slots []Slot, elements []InternalNodeElement) {

	freeSpaceBegin := uint16(codec.headerCodec.getHeaderSize())
	freeSpaceEnd := uint16(4096)

	for i := range slots {

		freeSpaceEnd = codec.appendElement(page, freeSpaceEnd, elements[i])
		slots[i].elementPointer = freeSpaceEnd
		freeSpaceBegin = codec.slotCodec.appendSlot(page, freeSpaceBegin, slots[i])

	}

	headerBytes := page[:codec.headerCodec.getHeaderSize()]

	codec.headerCodec.setNumSlots(headerBytes, len(slots))
	codec.headerCodec.setGarbageSize(headerBytes, 0)
	codec.headerCodec.setFreeSpaceBegin(headerBytes, freeSpaceBegin)
	codec.headerCodec.setFreeSpaceEnd(headerBytes, freeSpaceEnd)
	codec.headerCodec.SetIsPageFilled(headerBytes, true)
}

// DeleteElement is used to delete a key value pair, if it exists
func (codec InternalNodeCodec) DeleteElement(page []byte, key []byte) bool {

	slotBytes, elementBytes, found := codec.linearSearch(page, key)

	if found != 0 {
		return false
	}
	defer codec.headerCodec.updateCRC(page)

	// reset element pointer in slot
	codec.slotCodec.setElementPointer(slotBytes, codec.slotCodec.getDeletedElementPointerVal())

	// update garbage size
	headerBytes := page[:codec.headerCodec.getHeaderSize()]
	header := codec.headerCodec.decodePageHeader(headerBytes)

	codec.headerCodec.setGarbageSize(headerBytes, header.garbageSize+uint16(len(elementBytes)+codec.slotCodec.getSlotSize()))

	return true
}

// compact is used to remove all garbage that results from performing delete/update operations on the page
func (codec InternalNodeCodec) compact(page []byte) {

	slots, elements := codec.getAllSlotsAndElements(page)

	codec.putAllSlotsAndElements(page, slots, elements)
}

func (codec InternalNodeCodec) SplitNode(leftNode []byte, rightNode []byte, rightNodePageId uint64) (extraKey []byte) {

	defer codec.headerCodec.updateCRC(leftNode)
	defer codec.headerCodec.updateCRC(rightNode)

	leftNodeHeaderBytes := leftNode[:codec.headerCodec.getHeaderSize()]
	leftNodeHeader := codec.headerCodec.decodePageHeader(leftNodeHeaderBytes)

	rightNodeHeaderBytes := rightNode[:codec.headerCodec.getHeaderSize()]

	slots, elements := codec.getAllSlotsAndElements(leftNode)

	totalDataRegionSize := codec.headerCodec.getTotalDataRegionSize(slots)

	leftNodeDataRegionSize := uint16(0)

	index := 0
	for index < len(slots) && leftNodeDataRegionSize <= totalDataRegionSize/2 {

		leftNodeDataRegionSize += slots[index].elementSize
		index++
	}

	leftSlots := slots[:index]
	leftElements := elements[:index]

	rightSlots := slots[index:]
	rightElements := elements[index:]

	extraKey = elements[index].Key

	codec.putAllSlotsAndElements(leftNode, leftSlots, leftElements)
	codec.putAllSlotsAndElements(rightNode, rightSlots, rightElements)

	codec.headerCodec.setNextLeafNodePageId(rightNodeHeaderBytes, leftNodeHeader.nextLeafNodePageId)
	codec.headerCodec.setNextLeafNodePageId(leftNodeHeaderBytes, rightNodePageId)

	return extraKey

}

func (codec InternalNodeCodec) linearSearch(page []byte, key []byte) (slotBytes []byte, elementBytes []byte, found int) {

	header := codec.headerCodec.decodePageHeader(page[:codec.headerCodec.getHeaderSize()])

	pointer := codec.headerCodec.getHeaderSize()

	for range int(header.numSlots) {

		currSlotBytes := page[pointer : pointer+codec.slotCodec.getSlotSize()]
		currSlot := codec.slotCodec.decodeSlot(currSlotBytes)
		pointer += 4

		slotBytes = currSlotBytes

		if !codec.slotCodec.isElementDeleted(currSlot) {

			currElementBytes := page[currSlot.elementPointer : currSlot.elementPointer+currSlot.elementSize]
			currElement := codec.decodeElement(currElementBytes)

			elementBytes = currElementBytes

			result := bytes.Compare(currElement.Key, key)
			if result == 0 {
				return currSlotBytes, currElementBytes, result
			} else if result == 1 {
				return slotBytes, elementBytes, result
			}
		}
	}
	return nil, nil, -1
}

// calculateElementSize returns the total size of the element in the data region
func (codec InternalNodeCodec) calculateElementSize(element InternalNodeElement) (size uint16) {

	keyLengthFieldSize := 2
	keyFieldSize := len(element.Key)
	leftChildNodePageIdFieldSize := 8
	rightChildNodePageIfFieldSize := 8

	return uint16(keyLengthFieldSize + keyFieldSize + leftChildNodePageIdFieldSize + rightChildNodePageIfFieldSize)
}

// insertSlot inserts a slot into the slot region while maintaining the sorted nature of the slot region. It also returns the left and right child node page ID of the element after insertion
func (codec InternalNodeCodec) InsertSlot(page []byte, newSlot Slot, key []byte, leftChildNodePageId uint64, rightChildNodePageId uint64) (updatedFreeSpaceBegin uint16) {

	fmt.Println()
	slog.Info("Inserting slot into page...", "function", "InsertSlot", "at", "SlotCodec")
	// initialize pointer to beginning of slot region
	pointer := codec.headerCodec.config.headerSize

	// extract header bytes from page
	headerBytes := page[:codec.headerCodec.config.headerSize]

	// decode header from header bytes
	header := codec.headerCodec.decodePageHeader(headerBytes)

	// create a list to store all slots corresponding to elements with key greater than or equal to target key
	greaterSlots := []Slot{newSlot}

	smallerElementBytes := make([]byte, 0)
	greaterElementBytes := make([]byte, 0)

	greaterFound := false
	// create a list to store all slots representing deleted elements.
	// iterate through all slots
	for range int(header.numSlots) {

		// extract slot from page
		slotBytes := page[pointer : pointer+codec.slotCodec.getSlotSize()]

		// decode slot from slot bytes
		existingSlot := codec.slotCodec.decodeSlot(slotBytes)

		// if element is not deleted
		if !codec.slotCodec.isElementDeleted(existingSlot) {

			// extract element bytes from page
			elementBytes := page[existingSlot.elementPointer : existingSlot.elementPointer+existingSlot.elementSize]

			// decode elment from element bytes
			existingElement := codec.decodeElement(elementBytes)

			// compare element key and target key
			result := bytes.Compare(existingElement.Key, key)

			// if element.key > target key, append slot to greater slots list
			if result == 1 {
				if !greaterFound {
					greaterElementBytes = elementBytes
					greaterFound = true
				}
				greaterSlots = append(greaterSlots, existingSlot)
			} else if result == -1 {
				smallerElementBytes = elementBytes
			}

		} else {

			// if slot represents deleted element, but its key would have been greater than target key,
			// append to greater slots list in order to maintain numSlots value

			// If we dont do this, such slots would be skipped, and numSlots = numSlots + 1 would be an incorrect update
			// as we wouldnt be writing these deleted slots back to the page
			if greaterFound {
				greaterSlots = append(greaterSlots, existingSlot)
			}
		}

		pointer = pointer + codec.slotCodec.getSlotSize()
	}

	// calculate number of slots in page greater than new slot
	numSlotsGreater := len(greaterSlots) - 1

	// caluclate offset in page from which insertion of all slots in list should begin
	header.freeSpaceBegin = header.freeSpaceBegin - uint16(numSlotsGreater*codec.slotCodec.getSlotSize())

	// insert each slot into the page
	for _, currSlot := range greaterSlots {

		header.freeSpaceBegin = codec.slotCodec.appendSlot(page, header.freeSpaceBegin, currSlot)
	}

	if header.numSlots == 0 {
		return header.freeSpaceBegin
	}
	if len(greaterElementBytes) != 0 {
		codec.setLeftChildNodePageId(greaterElementBytes, rightChildNodePageId)
	}
	if len(smallerElementBytes) != 0 {
		codec.setRightChildNodePageId(smallerElementBytes, leftChildNodePageId)
	}

	// return updated free space begin pointer
	return header.freeSpaceBegin
}

func (codec InternalNodeCodec) PrintElements(page []byte) {

	_, elements := codec.getAllSlotsAndElements(page)

	for i, element := range elements {
		slog.Info(fmt.Sprintf("Element %d: Key: %s, LeftChildNodePageId: %d, RightChildNodePageId: %d", i, string(element.Key), element.LeftChildNodePageId, element.RightChildNodePageId), "function", "printElements", "at", "InternalNodeCodec")

	}
}

func (codec InternalNodeCodec) appendElement(page []byte, freeSpaceEnd uint16, element InternalNodeElement) (updatedFreeSpaceEnd uint16) {
	fmt.Println()
	slog.Info("Appending element to page", "key", string(element.Key), "leftChildNodePageId", element.LeftChildNodePageId, "rightChildNodePageId", element.RightChildNodePageId, "function", "appendElement", "at", "SlottedPageCodec")
	elementBytes := codec.encodeElement(element)

	copy(page[int(freeSpaceEnd)-len(elementBytes):], elementBytes)
	isPageEmpty(page[int(freeSpaceEnd)-len(elementBytes):])
	return freeSpaceEnd - uint16(len(elementBytes))

}

func (codec InternalNodeCodec) appendAllSlotsAndElements(page []byte, slots []Slot, elements []InternalNodeElement) {

	headerBytes := page[:codec.headerCodec.getHeaderSize()]

	header := codec.headerCodec.decodePageHeader(headerBytes)

	for i := range slots {

		header.freeSpaceEnd = codec.appendElement(page, header.freeSpaceEnd, elements[i])
		slots[i].elementPointer = header.freeSpaceEnd
		header.freeSpaceBegin = codec.slotCodec.appendSlot(page, header.freeSpaceBegin, slots[i])
	}

	codec.headerCodec.setNumSlots(headerBytes, int(header.numSlots)+len(slots))
	codec.headerCodec.setFreeSpaceBegin(headerBytes, header.freeSpaceBegin)
	codec.headerCodec.setFreeSpaceEnd(headerBytes, header.freeSpaceEnd)
	codec.headerCodec.setGarbageSize(headerBytes, 0)

}

// func (codec InternalNodeCodec) Merge(underflowNode []byte, separatorKey []byte, separatorValue []byte, siblingNode []byte, isLeftSibling bool) {

// 	defer codec.headerCodec.updateCRC(underflowNode)
// 	underflowSlots, underflowElements := codec.getAllSlotsAndElements(underflowNode)
// 	siblingSlots, siblingElements := codec.getAllSlotsAndElements(siblingNode)

// 	separatorElement := InternalNodeElement{
// 		Key:   separatorKey,
// 		Value: separatorValue,
// 	}

// 	separatorSlot := Slot{
// 		elementSize: codec.calculateElementSize(separatorElement),
// 	}

// 	var leftSlots, rightSlots []Slot
// 	var leftElements, rightElements []LeafNodeElement

// 	if isLeftSibling {
// 		separatorElement.LeftChildNodePageId = siblingElements[len(siblingElements)-1].RightChildNodePageId
// 		separatorElement.RightChildNodePageId = underflowElements[0].LeftChildNodePageId

// 		leftSlots = siblingSlots
// 		leftElements = siblingElements
// 		rightSlots = underflowSlots
// 		rightElements = underflowElements

// 	} else {
// 		separatorElement.LeftChildNodePageId = underflowElements[len(underflowElements)-1].RightChildNodePageId
// 		separatorElement.RightChildNodePageId = siblingElements[0].LeftChildNodePageId

// 		leftSlots = underflowSlots
// 		leftElements = underflowElements
// 		rightSlots = siblingSlots
// 		rightElements = siblingElements
// 	}

// 	codec.putAllSlotsAndElements(underflowNode, leftSlots, leftElements)

// 	headerBytes := underflowNode[:codec.headerCodec.getHeaderSize()]

// 	header := codec.headerCodec.decodePageHeader(headerBytes)

// 	separatorSlot.elementPointer = header.freeSpaceEnd - separatorSlot.elementSize

// 	codec.headerCodec.setFreeSpaceEnd(headerBytes, header.freeSpaceEnd-separatorSlot.elementSize)

// 	rightSlots = append([]Slot{separatorSlot}, rightSlots...)
// 	rightElements = append([]LeafNodeElement{separatorElement}, rightElements...)

// 	codec.appendAllSlotsAndElements(underflowNode, rightSlots, rightElements)

// }
