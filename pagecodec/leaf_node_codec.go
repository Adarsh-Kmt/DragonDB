package pagecodec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log/slog"
)

type LeafNodeCodec struct {
	slotCodec   SlotCodec
	headerCodec HeaderCodec
}
type LeafNodeElement struct {
	Key   []byte
	Value []byte
}

func NewLeafNodeCodec() LeafNodeCodec {

	return LeafNodeCodec{
		headerCodec: DefaultHeaderCodec(),
		slotCodec:   defaultSlotCodec(),
	}
}

// decodeElement takes a slice of bytes representing a element in the data region, and returns a deserialized element object
func (codec LeafNodeCodec) decodeElement(elementBytes []byte) LeafNodeElement {

	e := LeafNodeElement{}

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

	return e

}

// encodeSlot takes an element struct and returns an encoded slice of bytes representing this element
func (codec LeafNodeCodec) encodeElement(element LeafNodeElement) []byte {

	fmt.Println()
	//slog.Info("Encoding element...", "function", "encodeElement", "at", "LeafNodeCodec")
	b := make([]byte, 0)

	b = binary.LittleEndian.AppendUint16(b, uint16(len(element.Key)))

	b = append(b, element.Key...)

	b = binary.LittleEndian.AppendUint16(b, uint16(len(element.Value)))

	b = append(b, element.Value...)

	return b
}

func (codec LeafNodeCodec) SetNodeType(page []byte) {

	codec.headerCodec.SetNodeType(page[:codec.headerCodec.getHeaderSize()], true)
}

// setValue sets the value field in the element. only use if len(new_value) <= len(old_value)
func (codec LeafNodeCodec) setValueInElement(elementBytes []byte, value []byte) {

	fmt.Println()
	//slog.Info("Setting value in element...", "function", "setValue", "at", "LeafNodeCodec")
	pointer := 0

	keySize := binary.LittleEndian.Uint16(elementBytes[pointer:])
	pointer += 2

	pointer += int(keySize)

	binary.LittleEndian.PutUint16(elementBytes[pointer:], uint16(len(value)))
	pointer += 2

	copy(elementBytes[pointer:], value)

}

// FindElement is used to return the value corresponding to a key, or the next page ID where this key could be found
func (codec LeafNodeCodec) FindValue(page []byte, key []byte) (value []byte, found bool) {

	_, elements := codec.getAllSlotsAndElements(page)

	for _, element := range elements {

		result := bytes.Compare(element.Key, key)

		if result == 0 {
			return element.Value, true

		}
	}

	return nil, false
}

func (codec LeafNodeCodec) SetValue(page []byte, key []byte, value []byte) bool {
	defer codec.headerCodec.updateCRC(page)

	// search for slot, element corresponding to key
	slotBytes, elementBytes, _ := codec.linearSearch(page, key)

	// decode existing element
	oldElement := codec.decodeElement(elementBytes)

	// extract header bytes from page
	headerBytes := page[:codec.headerCodec.getHeaderSize()]

	// decode header
	header := codec.headerCodec.decodePageHeader(headerBytes)

	// calculate space required to store element
	elementSpaceRequired := 2 + len(key) + 2 + len(value)

	// if size(current_element_value) >= size(new_element_value)
	if len(oldElement.Value) >= len(value) {

		// update value in place
		codec.setValueInElement(elementBytes, value)

		// update garbage size field in the header region
		codec.headerCodec.setGarbageSize(headerBytes, header.garbageSize+uint16(len(oldElement.Value)-len(value)))

	} else {

		// this block is executed if size(current_element_value) < size(new_element_value)
		// in this case, a new element must be inserted into the free space region

		// check if free space region has enough space to accomodate new element.
		if !codec.headerCodec.isAdequate(page, elementSpaceRequired) {

			// if space is not enough, check if performing compaction will free up enough space to insert the element.
			if !codec.headerCodec.shouldCompact(page, elementSpaceRequired) {

				// if even compaction won't free up enough space to insert the new element, return false.
				return false
			} else {

				// if compaction is useful, perform compaction first before inserting the new element.
				codec.compact(page)

				// update the header after compaction
				headerBytes = page[:codec.headerCodec.getHeaderSize()]
				header = codec.headerCodec.decodePageHeader(headerBytes)
			}
		}

		// create element
		newElement := LeafNodeElement{
			Key:   key,
			Value: value,
		}

		// append value to end of free space region
		header.freeSpaceEnd = codec.appendElement(page, header.freeSpaceEnd, newElement)

		// update free space end value
		codec.headerCodec.setFreeSpaceEnd(headerBytes, header.freeSpaceEnd)

		// update element pointer field in existing slot
		codec.slotCodec.setElementPointer(slotBytes, header.freeSpaceEnd)

		// update element size field in existing slot
		codec.slotCodec.setElementSize(slotBytes, codec.calculateElementSize(newElement))

		// update garbage size field in header region
		codec.headerCodec.setGarbageSize(headerBytes, header.garbageSize+codec.calculateElementSize(oldElement))

	}
	return true
}

// InsertElement is used to insert a key value pair in a page
func (codec LeafNodeCodec) InsertElement(page []byte, key []byte, value []byte) bool {

	fmt.Println()

	defer codec.headerCodec.updateCRC(page)

	// extract header bytes from page
	headerBytes := page[:codec.headerCodec.getHeaderSize()]
	// slog.Info(fmt.Sprintf("extracted header %v of size %d", headerBytes, codec.headerCodec.getHeaderSize()))
	// slog.Info(fmt.Sprintf("header size %d", len(headerBytes)))
	// decode header
	header := codec.headerCodec.decodePageHeader(headerBytes)
	//slog.Info("Inserting element in page...", "key", string(key), "slots", header.numSlots, "function", "InsertElement", "at", "LeafNodeCodec")
	// calculate space required to store element
	elementSpaceRequired := 2 + len(key) + 2 + len(value)

	// calculate space required to store new slot
	slotSpaceRequired := codec.slotCodec.GetSlotSize()

	//slog.Info("LeafNodeElement space required", "size", elementSpaceRequired, "function", "InsertElement", "at", "LeafNodeCodec")

	// check if free space region has enough space to accomodate new element
	if !codec.headerCodec.isAdequate(page, elementSpaceRequired+slotSpaceRequired) {

		//slog.Info("Not enough space in free space region", "function", "InsertElement", "at", "LeafNodeCodec")
		//slog.Info("checking if compaction will help...", "function", "InsertElement", "at", "LeafNodeCodec")
		// if free space is not adequate, check if performing compaction will help
		if !codec.headerCodec.shouldCompact(page, elementSpaceRequired+slotSpaceRequired) {
			//slog.Info("Compaction will not help", "function", "InsertElement", "at", "LeafNodeCodec")
			// if compaction doesnt free up enough space, return false.
			return false
		} else {
			//slog.Info("Compaction will help, initiating compaction....", "function", "InsertElement", "at", "LeafNodeCodec")
			// if compaction frees up enough space to insert new element + new slot, perform compaction
			codec.compact(page)

			// update the header after compaction
			headerBytes = page[:codec.headerCodec.getHeaderSize()]
			header = codec.headerCodec.decodePageHeader(headerBytes)
		}
	}

	// create new element
	newElement := LeafNodeElement{
		Key:   key,
		Value: value,
	}

	// Debug: print page before append
	//fmt.Printf("[DEBUG] page before appendElement: %v\n", page)
	header.freeSpaceEnd = codec.appendElement(page, header.freeSpaceEnd, newElement)
	// Debug: print page after append
	//fmt.Printf("[DEBUG] page after appendElement: %v\n", page)

	// create new slot
	newSlot := Slot{
		elementSize:    codec.calculateElementSize(newElement),
		elementPointer: header.freeSpaceEnd,
	}

	header.freeSpaceBegin = codec.InsertSlot(page, newSlot, key)
	// update free space end pointer field in header region
	codec.headerCodec.setFreeSpaceEnd(headerBytes, header.freeSpaceEnd)
	// update free space begin pointer field in header region
	codec.headerCodec.setFreeSpaceBegin(headerBytes, header.freeSpaceBegin)
	// update number of slots field in header region

	fmt.Println("number of slots after inserting key = " + string(key) + " = " + string(int(header.numSlots)))
	codec.headerCodec.setNumSlots(headerBytes, int(header.numSlots)+1)
	codec.headerCodec.SetIsPageFilled(headerBytes, false)
	return true
}

// getAllSlotsAndElements returns a list of slots and their corresponding elements in the page. This function skips deleted elements
func (codec LeafNodeCodec) getAllSlotsAndElements(page []byte) ([]Slot, []LeafNodeElement) {

	header := codec.headerCodec.decodePageHeader(page)
	pointer := codec.headerCodec.getHeaderSize()

	slots := make([]Slot, 0)
	elements := make([]LeafNodeElement, 0)

	for range int(header.numSlots) {

		slotBytes := page[pointer : pointer+codec.slotCodec.GetSlotSize()]

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
func (codec LeafNodeCodec) putAllSlotsAndElements(page []byte, slots []Slot, elements []LeafNodeElement) {

	freeSpaceBegin := uint16(codec.headerCodec.getHeaderSize())
	freeSpaceEnd := uint16(4096)

	for i := range slots {
		//slog.Info(fmt.Sprintf("after split putting element key = %s", string(elements[i].Key)))
		freeSpaceEnd = codec.appendElement(page, freeSpaceEnd, elements[i])
		slots[i].elementPointer = freeSpaceEnd
		freeSpaceBegin = codec.slotCodec.appendSlot(page, freeSpaceBegin, slots[i])

	}

	headerBytes := page[:codec.headerCodec.getHeaderSize()]

	//slog.Info("total elements being set after split", "num-slots", len(slots))
	codec.headerCodec.setNumSlots(headerBytes, len(slots))
	codec.headerCodec.setGarbageSize(headerBytes, 0)
	codec.headerCodec.setFreeSpaceBegin(headerBytes, freeSpaceBegin)
	codec.headerCodec.setFreeSpaceEnd(headerBytes, freeSpaceEnd)
	codec.headerCodec.SetIsPageFilled(headerBytes, true)
}

// DeleteElement is used to delete a key value pair, if it exists
func (codec LeafNodeCodec) DeleteElement(page []byte, key []byte) bool {

	slotBytes, elementBytes, found := codec.linearSearch(page, key)

	if found != 0 {
		return false
	}
	defer codec.headerCodec.updateCRC(page)

	// reset element pointer in slot
	codec.slotCodec.setElementPointer(slotBytes, codec.slotCodec.GetDeletedElementPointerVal())

	// update garbage size
	headerBytes := page[:codec.headerCodec.getHeaderSize()]
	header := codec.headerCodec.decodePageHeader(headerBytes)

	codec.headerCodec.setGarbageSize(headerBytes, header.garbageSize+uint16(len(elementBytes)+codec.slotCodec.GetSlotSize()))

	return true
}

// compact is used to remove all garbage that results from performing delete/update operations on the page
func (codec LeafNodeCodec) compact(page []byte) {

	slots, elements := codec.getAllSlotsAndElements(page)

	codec.putAllSlotsAndElements(page, slots, elements)
}

func (codec LeafNodeCodec) SplitNode(leftNode []byte, rightNode []byte, rightNodePageId uint64) (extraKey []byte) {

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
	slog.Info("!!!!!!!!!!!!!!!")
	slog.Info("splitting node")
	leftSlots := slots[:index]
	leftElements := elements[:index]
	slog.Info(fmt.Sprintf("elements being moved to left node %d ", len(leftSlots)))
	rightSlots := slots[index:]
	rightElements := elements[index:]

	extraKey = elements[index].Key
	slog.Info("moving elements to left node")
	codec.putAllSlotsAndElements(leftNode, leftSlots, leftElements)
	slog.Info("moving elements to right node")
	codec.putAllSlotsAndElements(rightNode, rightSlots, rightElements)

	codec.headerCodec.setNextLeafNodePageId(rightNodeHeaderBytes, leftNodeHeader.nextLeafNodePageId)
	codec.headerCodec.setNextLeafNodePageId(leftNodeHeaderBytes, rightNodePageId)

	return extraKey

}

func (codec LeafNodeCodec) linearSearch(page []byte, key []byte) (slotBytes []byte, elementBytes []byte, found int) {

	header := codec.headerCodec.decodePageHeader(page[:codec.headerCodec.getHeaderSize()])

	pointer := codec.headerCodec.getHeaderSize()

	for range int(header.numSlots) {

		currSlotBytes := page[pointer : pointer+codec.slotCodec.GetSlotSize()]
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
func (codec LeafNodeCodec) calculateElementSize(element LeafNodeElement) (size uint16) {

	keyLengthFieldSize := 2
	keyFieldSize := len(element.Key)
	valueLengthFieldSize := 2
	valueFieldSize := len(element.Value)

	return uint16(keyLengthFieldSize + keyFieldSize + valueLengthFieldSize + valueFieldSize)
}

// insertSlot inserts a slot into the slot region while maintaining the sorted nature of the slot region. It also returns the left and right child node page ID of the element after insertion
func (codec LeafNodeCodec) InsertSlot(page []byte, newSlot Slot, key []byte) (updatedFreeSpaceBegin uint16) {

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

	greaterFound := false

	// iterate through all slots
	for range int(header.numSlots) {

		// extract slot from page
		slotBytes := page[pointer : pointer+codec.slotCodec.GetSlotSize()]

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

					greaterFound = true
				}
				greaterSlots = append(greaterSlots, existingSlot)
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

		pointer = pointer + codec.slotCodec.GetSlotSize()
	}

	// calculate number of slots in page greater than new slot
	numSlotsGreater := len(greaterSlots) - 1

	// caluclate offset in page from which insertion of all slots in list should begin
	header.freeSpaceBegin = header.freeSpaceBegin - uint16(numSlotsGreater*codec.slotCodec.GetSlotSize())

	// insert each slot into the page
	for _, currSlot := range greaterSlots {

		header.freeSpaceBegin = codec.slotCodec.appendSlot(page, header.freeSpaceBegin, currSlot)
	}

	if header.numSlots == 0 {
		return header.freeSpaceBegin
	}

	// return updated free space begin pointer
	return header.freeSpaceBegin
}

func (codec LeafNodeCodec) PrintElements(page []byte) {

	_, elements := codec.getAllSlotsAndElements(page)

	for i, element := range elements {
		slog.Info(fmt.Sprintf("Element %d: Key: %s ", i, string(element.Key)), "function", "printElements", "at", "LeafNodeCodec")

	}
}

func (codec LeafNodeCodec) appendElement(page []byte, freeSpaceEnd uint16, element LeafNodeElement) (updatedFreeSpaceEnd uint16) {
	fmt.Println()
	slog.Info("Appending element to page", "key", string(element.Key), "function", "appendElement", "at", "SlottedPageCodec")
	elementBytes := codec.encodeElement(element)
	// Debug: print elementBytes
	//fmt.Printf("[DEBUG] elementBytes: %v\n", elementBytes)
	writeStart := int(freeSpaceEnd) - len(elementBytes)
	writeEnd := int(freeSpaceEnd)
	//fmt.Printf("[DEBUG] Writing to page[%d:%d]\n", writeStart, writeEnd)
	copy(page[writeStart:writeEnd], elementBytes)
	// Debug: print written region
	//fmt.Printf("[DEBUG] page[%d:%d] after write: %v\n", writeStart, writeEnd, page[writeStart:writeEnd])
	//fmt.Printf("[DEBUG] isPageEmpty(page): %v\n", isPageEmpty(page))

	return freeSpaceEnd - uint16(len(elementBytes))

}

func (codec LeafNodeCodec) appendAllSlotsAndElements(page []byte, slots []Slot, elements []LeafNodeElement) {

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
	codec.headerCodec.SetIsPageFilled(headerBytes, true)

}

func (codec LeafNodeCodec) GetNextLeafNodePageId(page []byte) uint64 {

	headerBytes := page[:codec.headerCodec.getHeaderSize()]

	header := codec.headerCodec.decodePageHeader(headerBytes)
	return header.nextLeafNodePageId
}

func (codec LeafNodeCodec) IsSlotDeleted(page []byte, index int) bool {

	slots, _ := codec.getAllSlotsAndElements(page)

	return codec.slotCodec.isElementDeleted(slots[index])
}

func (codec LeafNodeCodec) GetNumSlots(page []byte) uint16 {

	headerBytes := page[:codec.headerCodec.getHeaderSize()]

	header := codec.headerCodec.decodePageHeader(headerBytes)
	return header.numSlots
}

func (codec LeafNodeCodec) GetValueCorrespondingToSlot(page []byte, index uint16) []byte {

	_, elements := codec.getAllSlotsAndElements(page)

	return elements[index].Value
}

// func (codec LeafNodeCodec) Merge(underflowNode []byte, separatorKey []byte, separatorValue []byte, siblingNode []byte, isLeftSibling bool) {

// 	defer codec.headerCodec.updateCRC(underflowNode)
// 	underflowSlots, underflowElements := codec.getAllSlotsAndElements(underflowNode)
// 	siblingSlots, siblingElements := codec.getAllSlotsAndElements(siblingNode)

// 	separatorElement := LeafNodeElement{
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
