package pagecodec

import (
	"encoding/binary"
	"fmt"
	"log/slog"
)

type SlotCodec struct {
	config      SlotConfig
	headerCodec HeaderCodec
}
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

func defaultSlotCodec() SlotCodec {

	return SlotCodec{
		config: defaultSlotConfig(),
	}
}

func (codec SlotCodec) getSlotSize() int {
	return codec.config.slotSize
}
func (codec SlotCodec) getDeletedElementPointerVal() uint16 {
	return codec.config.deletedElementPointerVal
}

// decodeSlot takes a slice of bytes representing a slot, and returns a decoded slot struct
func (codec SlotCodec) decodeSlot(slotBytes []byte) Slot {

	fmt.Println()
	slog.Info("Decoding slot...", "function", "decodeSlot", "at", "SlotCodec")
	s := Slot{}

	s.elementSize = binary.LittleEndian.Uint16(slotBytes[codec.config.elementSizeOffset:])
	s.elementPointer = binary.LittleEndian.Uint16(slotBytes[codec.config.elementPointerOffset:])

	slog.Info("Decoded slot", "elementSize", s.elementSize, "elementPointer", s.elementPointer, "function", "decodeSlot", "at", "SlotCodec")
	return s
}

// encodeSlot takes a slot struct and returns an encoded slice of bytes representing this slot
func (codec SlotCodec) encodeSlot(slot Slot) []byte {

	fmt.Println()
	slog.Info("Encoding slot...", "function", "encodeSlot", "at", "SlotCodec")
	b := make([]byte, 0)

	b = binary.LittleEndian.AppendUint16(b, slot.elementSize)
	b = binary.LittleEndian.AppendUint16(b, slot.elementPointer)

	return b
}

// setElementPointer is used to set the value of the element pointer field in the slot
func (codec SlotCodec) setElementPointer(slotBytes []byte, p uint16) {

	binary.LittleEndian.PutUint16(slotBytes[codec.config.elementPointerOffset:], p)
}

// setElementSize is used to set the value of the element size field in the slot
func (codec SlotCodec) setElementSize(slotBytes []byte, s uint16) {

	binary.LittleEndian.PutUint16(slotBytes[codec.config.elementSizeOffset:], s)
}

// isElementDeleted is used to check if the slot points to a deleted element
func (codec SlotCodec) isElementDeleted(slot Slot) bool {
	return slot.elementPointer == codec.config.deletedElementPointerVal
}

// appendSlot is used to insert a slot at a particular offset in the page
func (codec SlotCodec) appendSlot(page []byte, freeSpaceBegin uint16, slot Slot) (updatedFreeSpaceBegin uint16) {

	fmt.Println()
	slog.Info("Appending slot to page...", "function", "appendSlot", "at", "SlotCodec")
	slotBytes := codec.encodeSlot(slot)
	copy(page[freeSpaceBegin:], slotBytes)

	return freeSpaceBegin + 4
}
