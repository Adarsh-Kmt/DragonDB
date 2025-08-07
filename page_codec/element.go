package page_codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log/slog"
)

type SlottedPageCodec struct {
	headerConfig HeaderConfig
	slotConfig   SlotConfig
}

type Element struct {
	Key                  []byte
	Value                []byte
	LeftChildNodePageId  uint64
	RightChildNodePageId uint64
}

func DefaultSlottedPageCodec() SlottedPageCodec {

	return SlottedPageCodec{
		headerConfig: defaultHeaderConfig(),
		slotConfig:   defaultSlotConfig(),
	}
}

// decodeElement takes a slice of bytes representing a element in the data region, and returns a deserialized element object
func (codec SlottedPageCodec) decodeElement(elementBytes []byte) Element {

	e := Element{}

	pointer := uint16(0)

	// decode key length field
	keyLength := binary.LittleEndian.Uint16(elementBytes[pointer:])
	pointer += 2

	key := make([]byte, keyLength)

	// extract key
	copy(key, elementBytes[pointer:pointer+keyLength])
	e.Key = key

	pointer += keyLength

	// decode value length field
	valueLength := binary.LittleEndian.Uint16(elementBytes[pointer:])
	pointer += 2

	value := make([]byte, valueLength)

	// extract value
	copy(value, elementBytes[pointer:pointer+valueLength])
	e.Value = value

	pointer += valueLength

	// decode left child node page ID
	e.LeftChildNodePageId = binary.LittleEndian.Uint64(elementBytes[pointer:])
	pointer += 8

	// decode right child node page ID
	e.RightChildNodePageId = binary.LittleEndian.Uint64(elementBytes[pointer:])

	return e

}

// encodeSlot takes an element struct and returns an encoded slice of bytes representing this element
func (codec SlottedPageCodec) encodeElement(element Element) []byte {

	fmt.Println()
	slog.Info("Encoding element...", "function", "encodeElement", "at", "SlottedPageCodec")
	b := make([]byte, 0)

	b = binary.LittleEndian.AppendUint16(b, uint16(len(element.Key)))

	b = append(b, element.Key...)

	b = binary.LittleEndian.AppendUint16(b, uint16(len(element.Value)))

	b = append(b, element.Value...)

	b = binary.LittleEndian.AppendUint64(b, element.LeftChildNodePageId)

	b = binary.LittleEndian.AppendUint64(b, element.RightChildNodePageId)

	return b
}

// setValue sets the value field in the element. only use if len(new_value) <= len(old_value)
func (codec SlottedPageCodec) setValue(elementBytes []byte, value []byte) {

	fmt.Println()
	slog.Info("Setting value in element...", "function", "setValue", "at", "SlottedPageCodec")
	pointer := 0

	keySize := binary.LittleEndian.Uint16(elementBytes[pointer:])
	pointer += 2

	pointer += int(keySize)

	binary.LittleEndian.PutUint16(elementBytes[pointer:], uint16(len(value)))
	pointer += 2

	copy(elementBytes[pointer:], value)

}

func (codec SlottedPageCodec) setRightChildNodePageId(elementBytes []byte, rightChildNodePageId uint64) {

	pointer := 0

	keySize := binary.LittleEndian.Uint16(elementBytes[pointer:])
	pointer += 2

	pointer += int(keySize)

	valueSize := binary.LittleEndian.Uint16(elementBytes[pointer:])
	pointer += 2
	pointer += int(valueSize)

	pointer += 8

	binary.LittleEndian.PutUint64(elementBytes[pointer:], rightChildNodePageId)

}

func (codec SlottedPageCodec) setLeftChildNodePageId(elementBytes []byte, leftChildNodePageId uint64) {

	pointer := 0

	keySize := binary.LittleEndian.Uint16(elementBytes[pointer:])
	pointer += 2

	pointer += int(keySize)

	valueSize := binary.LittleEndian.Uint16(elementBytes[pointer:])
	pointer += 2

	pointer += int(valueSize)

	binary.LittleEndian.PutUint64(elementBytes[pointer:], leftChildNodePageId)

}

// FindElement is used to return the value corresponding to a key, or the next page ID where this key could be found
func (codec SlottedPageCodec) FindElement(page []byte, key []byte) (value []byte, nextPageId uint64, found bool) {

	_, elements := codec.getAllSlotsAndElements(page)

	headerBytes := page[:codec.headerConfig.headerSize]

	header := codec.decodePageHeader(headerBytes)
	for _, element := range elements {

		result := bytes.Compare(element.Key, key)

		if result == 0 {
			return element.Value, 0, true

		} else if result == 1 {
			if header.isLeafNode {
				return nil, 0, false
			} else {
				return nil, element.LeftChildNodePageId, false
			}
		} else {
			nextPageId = element.RightChildNodePageId
		}
	}

	if header.isLeafNode {
		return nil, 0, false
	}

	return nil, nextPageId, false
}

func (codec SlottedPageCodec) SetValue(page []byte, key []byte, value []byte) bool {
	defer codec.updateCRC(page)

	// search for slot, element corresponding to key
	slotBytes, elementBytes, _ := codec.linearSearch(page, key)

	// decode existing element
	oldElement := codec.decodeElement(elementBytes)

	// extract header bytes from page
	headerBytes := page[:codec.headerConfig.headerSize]

	// decode header
	header := codec.decodePageHeader(headerBytes)

	// calculate space required to store element
	elementSpaceRequired := 2 + len(key) + 2 + len(value) + 8 + 8

	// if size(current_element_value) >= size(new_element_value)
	if len(oldElement.Value) >= len(value) {

		// update value in place
		codec.setValue(elementBytes, value)

		// update garbage size field in the header region
		codec.setGarbageSize(headerBytes, header.garbageSize+uint16(len(oldElement.Value)-len(value)))

	} else {

		// this block is executed if size(current_element_value) < size(new_element_value)
		// in this case, a new element must be inserted into the free space region

		// check if free space region has enough space to accomodate new element.
		if !codec.isAdequate(page, elementSpaceRequired) {

			// if space is not enough, check if performing compaction will free up enough space to insert the element.
			if !codec.shouldCompact(page, elementSpaceRequired) {

				// if even compaction won't free up enough space to insert the new element, return false.
				return false
			} else {

				// if compaction is useful, perform compaction first before inserting the new element.
				codec.compact(page)

				// update the header after compaction
				headerBytes = page[:codec.headerConfig.headerSize]
				header = codec.decodePageHeader(headerBytes)
			}
		}

		// create element
		newElement := Element{
			Key:                  key,
			Value:                value,
			LeftChildNodePageId:  oldElement.LeftChildNodePageId,
			RightChildNodePageId: oldElement.RightChildNodePageId,
		}

		// append value to end of free space region
		header.freeSpaceEnd = codec.appendElement(page, header.freeSpaceEnd, newElement)

		// update free space end value
		codec.setFreeSpaceEnd(headerBytes, header.freeSpaceEnd)

		// update element pointer field in existing slot
		codec.setElementPointer(slotBytes, header.freeSpaceEnd)

		// update element size field in existing slot
		codec.setElementSize(slotBytes, codec.calculateElementSize(newElement))

		// update garbage size field in header region
		codec.setGarbageSize(headerBytes, header.garbageSize+codec.calculateElementSize(oldElement))

	}
	return true
}

// InsertElement is used to insert a key value pair in a page
func (codec SlottedPageCodec) InsertElement(page []byte, key []byte, value []byte, leftChildNodePageId uint64, rightChildNodePageId uint64) bool {

	fmt.Println()
	slog.Info("Inserting element in page...", "key", string(key), "value", string(value), "left_child_node_page_ID", leftChildNodePageId, "right_child_node_page_ID", rightChildNodePageId, "function", "InsertElement", "at", "SlottedPageCodec")
	defer codec.updateCRC(page)

	// extract header bytes from page
	headerBytes := page[:codec.headerConfig.headerSize]

	// decode header
	header := codec.decodePageHeader(headerBytes)

	// calculate space required to store element
	elementSpaceRequired := 2 + len(key) + 2 + len(value) + 8 + 8

	// calculate space required to store new slot
	slotSpaceRequired := codec.slotConfig.slotSize

	slog.Info("Element space required", "size", elementSpaceRequired, "function", "InsertElement", "at", "SlottedPageCodec")

	// check if free space region has enough space to accomodate new element
	if !codec.isAdequate(page, elementSpaceRequired+slotSpaceRequired) {

		slog.Info("Not enough space in free space region", "function", "InsertElement", "at", "SlottedPageCodec")
		slog.Info("checking if compaction will help...", "function", "InsertElement", "at", "SlottedPageCodec")
		// if free space is not adequate, check if performing compaction will help
		if !codec.shouldCompact(page, elementSpaceRequired+slotSpaceRequired) {
			slog.Info("Compaction will not help", "function", "InsertElement", "at", "SlottedPageCodec")
			// if compaction doesnt free up enough space, return false.
			return false
		} else {
			slog.Info("Compaction will help, initiating compaction....", "function", "InsertElement", "at", "SlottedPageCodec")
			// if compaction frees up enough space to insert new element + new slot, perform compaction
			codec.compact(page)

			// update the header after compaction
			headerBytes = page[:codec.headerConfig.headerSize]
			header = codec.decodePageHeader(headerBytes)
		}
	}

	// create new element
	newElement := Element{
		Key:                  key,
		Value:                value,
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

	header.freeSpaceBegin = codec.InsertSlot(page, newSlot, newElement)
	// update free space end pointer field in header region
	codec.setFreeSpaceEnd(headerBytes, header.freeSpaceEnd)
	// update free space begin pointer field in header region
	codec.setFreeSpaceBegin(headerBytes, header.freeSpaceBegin)
	// update number of slots field in header region
	codec.setNumSlots(headerBytes, int(header.numSlots)+1)
	return true
}

// DeleteElement is used to delete a key value pair, if it exists
func (codec SlottedPageCodec) DeleteElement(page []byte, key []byte) bool {

	slotBytes, elementBytes, found := codec.linearSearch(page, key)

	if found != 0 {
		return false
	}
	defer codec.updateCRC(page)

	// reset element pointer in slot
	codec.setElementPointer(slotBytes, codec.slotConfig.deletedElementPointerVal)

	// update garbage size
	headerBytes := page[:codec.headerConfig.headerSize]
	header := codec.decodePageHeader(headerBytes)

	codec.setGarbageSize(headerBytes, header.garbageSize+uint16(len(elementBytes)+codec.slotConfig.slotSize))

	return true
}

// isAdequate is used to check whether the page has the required amount of free space or not
func (codec SlottedPageCodec) isAdequate(page []byte, spaceRequired int) bool {

	header := codec.decodePageHeader(page)

	freeSpace := header.freeSpaceEnd - header.freeSpaceBegin

	slog.Info("Checking if page has enough space...", "requiredSpace", spaceRequired, "freeSpace", freeSpace, "function", "isAdequate", "at", "SlottedPageCodec")
	return freeSpace >= uint16(spaceRequired)
}

// shoudCompact is used to check whether compaction will free up the required amount of space or not
func (codec SlottedPageCodec) shouldCompact(page []byte, size int) bool {

	header := codec.decodePageHeader(page)

	return size <= int(header.garbageSize)
}

// getAllSlotsAndElements returns a list of slots and their corresponding elements in the page. This function skips deleted elements
func (codec SlottedPageCodec) getAllSlotsAndElements(page []byte) ([]Slot, []Element) {

	header := codec.decodePageHeader(page)
	pointer := codec.headerConfig.headerSize

	slots := make([]Slot, 0)
	elements := make([]Element, 0)

	for range int(header.numSlots) {

		slotBytes := page[pointer : pointer+codec.slotConfig.slotSize]

		slot := codec.decodeSlot(slotBytes)
		pointer += 4

		if !codec.isElementDeleted(slot) {

			elementBytes := page[slot.elementPointer : slot.elementPointer+slot.elementSize]
			element := codec.decodeElement(elementBytes)
			slots = append(slots, slot)
			elements = append(elements, element)
		}
	}

	return slots, elements
}

// putAllSlotsAndElements inserts slots and elements into the page, assuming it to be empty
func (codec SlottedPageCodec) putAllSlotsAndElements(page []byte, slots []Slot, elements []Element) {

	freeSpaceBegin := uint16(codec.headerConfig.headerSize)
	freeSpaceEnd := uint16(4096)

	for i := range slots {

		freeSpaceEnd = codec.appendElement(page, freeSpaceEnd, elements[i])
		slots[i].elementPointer = freeSpaceEnd
		freeSpaceBegin = codec.appendSlot(page, freeSpaceBegin, slots[i])

	}

	headerBytes := page[:codec.headerConfig.headerSize]

	codec.setNumSlots(headerBytes, len(slots))
	codec.setGarbageSize(headerBytes, 0)
	codec.setFreeSpaceBegin(headerBytes, freeSpaceBegin)
	codec.setFreeSpaceEnd(headerBytes, freeSpaceEnd)
}

// compact is used to remove all garbage that results from performing delete/update operations on the page
func (codec SlottedPageCodec) compact(page []byte) {

	slots, elements := codec.getAllSlotsAndElements(page)

	codec.putAllSlotsAndElements(page, slots, elements)
}

// getTotalDataRegionSize returns the size of the data region
func (codec SlottedPageCodec) getTotalDataRegionSize(slots []Slot) uint16 {

	size := uint16(0)

	for _, slot := range slots {
		size += slot.elementSize
	}
	return size
}

func (codec SlottedPageCodec) Split(page []byte, rightNode []byte) (extraKey []byte, extraValue []byte) {

	defer codec.updateCRC(page)
	defer codec.updateCRC(rightNode)

	slots, elements := codec.getAllSlotsAndElements(page)

	totalDataRegionSize := codec.getTotalDataRegionSize(slots)

	leftNodeDataRegionSize := uint16(0)

	index := 0
	for index < len(slots) && leftNodeDataRegionSize <= totalDataRegionSize/2 {

		leftNodeDataRegionSize += slots[index].elementSize
		index++
	}

	leftSlots := slots[:index]
	leftElements := elements[:index]

	rightSlots := slots[index+1:]
	rightElements := elements[index+1:]

	extraKey = elements[index].Key
	extraValue = elements[index].Value

	codec.putAllSlotsAndElements(page, leftSlots, leftElements)
	codec.putAllSlotsAndElements(rightNode, rightSlots, rightElements)

	return extraKey, extraValue
}

func (codec SlottedPageCodec) linearSearch(page []byte, key []byte) (slotBytes []byte, elementBytes []byte, found int) {

	header := codec.decodePageHeader(page[:codec.headerConfig.headerSize])

	pointer := codec.headerConfig.headerSize

	for range int(header.numSlots) {

		currSlotBytes := page[pointer : pointer+codec.slotConfig.slotSize]
		currSlot := codec.decodeSlot(currSlotBytes)
		pointer += 4

		slotBytes = currSlotBytes

		if !codec.isElementDeleted(currSlot) {

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

func (codec SlottedPageCodec) appendElement(page []byte, freeSpaceEnd uint16, element Element) (updatedFreeSpaceEnd uint16) {
	fmt.Println()
	slog.Info("Appending element to page", "key", string(element.Key), "value", string(element.Value), "leftChildNodePageId", element.LeftChildNodePageId, "rightChildNodePageId", element.RightChildNodePageId, "function", "appendElement", "at", "SlottedPageCodec")
	elementBytes := codec.encodeElement(element)

	copy(page[int(freeSpaceEnd)-len(elementBytes):], elementBytes)

	return freeSpaceEnd - uint16(len(elementBytes))

}

func (codec SlottedPageCodec) appendAllSlotsAndElements(page []byte, slots []Slot, elements []Element) {

	headerBytes := page[:codec.headerConfig.headerSize]

	header := codec.decodePageHeader(headerBytes)

	for i := range slots {

		header.freeSpaceEnd = codec.appendElement(page, header.freeSpaceEnd, elements[i])
		slots[i].elementPointer = header.freeSpaceEnd
		header.freeSpaceBegin = codec.appendSlot(page, header.freeSpaceBegin, slots[i])
	}

	codec.setNumSlots(headerBytes, int(header.numSlots)+len(slots))
	codec.setFreeSpaceBegin(headerBytes, header.freeSpaceBegin)
	codec.setFreeSpaceEnd(headerBytes, header.freeSpaceEnd)
	codec.setGarbageSize(headerBytes, 0)

}
func (codec SlottedPageCodec) Merge(underflowNode []byte, separatorKey []byte, separatorValue []byte, siblingNode []byte, isLeftSibling bool) {

	defer codec.updateCRC(underflowNode)
	underflowSlots, underflowElements := codec.getAllSlotsAndElements(underflowNode)
	siblingSlots, siblingElements := codec.getAllSlotsAndElements(siblingNode)

	separatorElement := Element{
		Key:   separatorKey,
		Value: separatorValue,
	}

	separatorSlot := Slot{
		elementSize: codec.calculateElementSize(separatorElement),
	}

	var leftSlots, rightSlots []Slot
	var leftElements, rightElements []Element

	if isLeftSibling {
		separatorElement.LeftChildNodePageId = siblingElements[len(siblingElements)-1].RightChildNodePageId
		separatorElement.RightChildNodePageId = underflowElements[0].LeftChildNodePageId

		leftSlots = siblingSlots
		leftElements = siblingElements
		rightSlots = underflowSlots
		rightElements = underflowElements

	} else {
		separatorElement.LeftChildNodePageId = underflowElements[len(underflowElements)-1].RightChildNodePageId
		separatorElement.RightChildNodePageId = siblingElements[0].LeftChildNodePageId

		leftSlots = underflowSlots
		leftElements = underflowElements
		rightSlots = siblingSlots
		rightElements = siblingElements
	}

	codec.putAllSlotsAndElements(underflowNode, leftSlots, leftElements)

	headerBytes := underflowNode[:codec.headerConfig.headerSize]

	header := codec.decodePageHeader(headerBytes)

	separatorSlot.elementPointer = header.freeSpaceEnd - separatorSlot.elementSize

	codec.setFreeSpaceEnd(headerBytes, header.freeSpaceEnd-separatorSlot.elementSize)

	rightSlots = append([]Slot{separatorSlot}, rightSlots...)
	rightElements = append([]Element{separatorElement}, rightElements...)

	codec.appendAllSlotsAndElements(underflowNode, rightSlots, rightElements)

}

// calculateElementSize returns the total size of the element in the data region
func (codec SlottedPageCodec) calculateElementSize(element Element) (size uint16) {

	keyLengthFieldSize := 2
	keyFieldSize := len(element.Key)
	valueLengthFieldSize := 2
	valueFieldSize := len(element.Value)
	leftChildNodePageIdFieldSize := 8
	rightChildNodePageIfFieldSize := 8

	return uint16(keyLengthFieldSize + keyFieldSize + valueLengthFieldSize + valueFieldSize + leftChildNodePageIdFieldSize + rightChildNodePageIfFieldSize)
}

func (codec SlottedPageCodec) PrintElements(page []byte) {

	_, elements := codec.getAllSlotsAndElements(page)

	for i, element := range elements {
		slog.Info(fmt.Sprintf("Element %d: Key: %s, LeftChildNodePageId: %d, RightChildNodePageId: %d", i, string(element.Key), element.LeftChildNodePageId, element.RightChildNodePageId), "function", "printElements", "at", "SlottedPageCodec")

	}
}
