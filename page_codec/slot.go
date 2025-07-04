package page_codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log/slog"
)

type Slot struct {
	elementSize    uint16
	elementPointer uint16
}

type SlotConfig struct {

	// slot field offsets within slot
	elementPointerOffset int
	elementSizeOffset    int

	// constants
	slotSize                 int
	deletedElementPointerVal uint16
}

func defaultSlotConfig() SlotConfig {

	return SlotConfig{
		elementSizeOffset:    0,
		elementPointerOffset: 2,

		slotSize:                 4,
		deletedElementPointerVal: 0xFFFF,
	}
}

// decodeSlot takes a slice of bytes representing a slot, and returns a decoded slot struct
func (codec SlottedPageCodec) decodeSlot(slotBytes []byte) Slot {

	fmt.Println()
	slog.Info("Decoding slot...", "function", "decodeSlot", "at", "SlottedPageCodec")
	s := Slot{}

	s.elementSize = binary.LittleEndian.Uint16(slotBytes[codec.slotConfig.elementSizeOffset:])
	s.elementPointer = binary.LittleEndian.Uint16(slotBytes[codec.slotConfig.elementPointerOffset:])

	slog.Info("Decoded slot", "elementSize", s.elementSize, "elementPointer", s.elementPointer, "function", "decodeSlot", "at", "SlottedPageCodec")
	return s
}

// encodeSlot takes a slot struct and returns an encoded slice of bytes representing this slot
func (codec SlottedPageCodec) encodeSlot(slot Slot) []byte {

	fmt.Println()
	slog.Info("Encoding slot...", "function", "encodeSlot", "at", "SlottedPageCodec")
	b := make([]byte, 0)

	b = binary.LittleEndian.AppendUint16(b, slot.elementSize)
	b = binary.LittleEndian.AppendUint16(b, slot.elementPointer)

	return b
}

// setElementPointer is used to set the value of the element pointer field in the slot
func (codec SlottedPageCodec) setElementPointer(slotBytes []byte, p uint16) {

	binary.LittleEndian.PutUint16(slotBytes[codec.slotConfig.elementPointerOffset:], p)
}

// setElementSize is used to set the value of the element size field in the slot
func (codec SlottedPageCodec) setElementSize(slotBytes []byte, s uint16) {

	binary.LittleEndian.PutUint16(slotBytes[codec.slotConfig.elementSizeOffset:], s)
}

// isElementDeleted is used to check if the slot points to a deleted element
func (codec SlottedPageCodec) isElementDeleted(slot Slot) bool {
	return slot.elementPointer == codec.slotConfig.deletedElementPointerVal
}

// appendSlot is used to insert a slot at a particular offset in the page
func (codec SlottedPageCodec) appendSlot(page []byte, freeSpaceBegin uint16, slot Slot) (updatedFreeSpaceBegin uint16) {

	fmt.Println()
	slog.Info("Appending slot to page...", "function", "appendSlot", "at", "SlottedPageCodec")
	slotBytes := codec.encodeSlot(slot)
	copy(page[freeSpaceBegin:], slotBytes)

	return freeSpaceBegin + 4
}

// insertSlot inserts a slot into the slot region while maintaining the sorted nature of the slot region. It also returns the left and right child node page ID of the element after insertion
func (codec SlottedPageCodec) InsertSlot(page []byte, newSlot Slot, newElement Element) (updatedFreeSpaceBegin uint16) {

	fmt.Println()
	slog.Info("Inserting slot into page...", "function", "InsertSlot", "at", "SlottedPageCodec")
	// initialize pointer to beginning of slot region
	pointer := codec.headerConfig.headerSize

	// extract header bytes from page
	headerBytes := page[:codec.headerConfig.headerSize]

	// decode header from header bytes
	header := codec.decodePageHeader(headerBytes)

	// create a list to store all slots corresponding to elements with key greater than or equal to target key
	greaterSlots := []Slot{newSlot}

	smallerElementBytes := make([]byte, 0)
	greaterElementBytes := make([]byte, 0)

	greaterFound := false
	// create a list to store all slots representing deleted elements.
	// iterate through all slots
	for range int(header.numSlots) {

		// extract slot from page
		slotBytes := page[pointer : pointer+codec.slotConfig.slotSize]

		// decode slot from slot bytes
		existingSlot := codec.decodeSlot(slotBytes)

		// if element is not deleted
		if !codec.isElementDeleted(existingSlot) {

			// extract element bytes from page
			elementBytes := page[existingSlot.elementPointer : existingSlot.elementPointer+existingSlot.elementSize]

			// decode elment from element bytes
			existingElement := codec.decodeElement(elementBytes)

			// compare element key and target key
			result := bytes.Compare(existingElement.Key, newElement.Key)

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

		pointer = pointer + codec.slotConfig.slotSize
	}

	// calculate number of slots in page greater than new slot
	numSlotsGreater := len(greaterSlots) - 1

	// caluclate offset in page from which insertion of all slots in list should begin
	header.freeSpaceBegin = header.freeSpaceBegin - uint16(numSlotsGreater*codec.slotConfig.slotSize)

	// insert each slot into the page
	for _, currSlot := range greaterSlots {

		header.freeSpaceBegin = codec.appendSlot(page, header.freeSpaceBegin, currSlot)
	}

	if header.numSlots == 0 {
		return header.freeSpaceBegin
	}
	if len(greaterElementBytes) != 0 {
		codec.setLeftChildNodePageId(greaterElementBytes, newElement.RightChildNodePageId)
	}
	if len(smallerElementBytes) != 0 {
		codec.setRightChildNodePageId(smallerElementBytes, newElement.LeftChildNodePageId)
	}

	// return updated free space begin pointer
	return header.freeSpaceBegin
}
