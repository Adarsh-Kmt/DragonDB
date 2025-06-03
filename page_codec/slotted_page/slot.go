package pagecodec

import "encoding/binary"

type SlotConfig struct {

	// slot field offsets
	elementPointerOffset int
	elementSizeOffset    int

	// constants
	slotSize                 int
	deletedElementPointerVal uint16
}

type Slot struct {
	elementSize    uint16
	elementPointer uint16
}

// slot region functions
func (codec *SlottedPageCodec) getSlot(page []byte, offset int) Slot {

	s := Slot{}

	pointer := offset
	s.elementSize = binary.LittleEndian.Uint16(page[pointer:])

	pointer += 2
	s.elementPointer = binary.LittleEndian.Uint16(page[pointer:])

	return s
}

func (codec *SlottedPageCodec) isElementDeleted(slot Slot) bool {

	if slot.elementPointer == 0xFFFF {
		return true
	}

	return false
}

func (codec *SlottedPageCodec) putSlotAtOffset(page []byte, freeSpaceBegin int, slot Slot) (updatedFreeSpaceBegin int) {

	slotBytes := codec.createSlot(slot)
	copy(page[freeSpaceBegin:], slotBytes)

	updatedFreeSpaceBegin = freeSpaceBegin + 4
	return
}

func (codec *SlottedPageCodec) createSlot(slot Slot) []byte {

	b := make([]byte, 0)

	b = binary.LittleEndian.AppendUint16(b, slot.elementSize)
	b = binary.LittleEndian.AppendUint16(b, slot.elementPointer)

	return b
}
