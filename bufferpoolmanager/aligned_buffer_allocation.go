package bufferpoolmanager

import "unsafe"

// returns offset in page where starting address of buffer begins.
func findOffset(buffer []byte) uintptr {
	return uintptr(unsafe.Pointer(&buffer[0])) % uintptr(4096)
}

func isAligned(buffer []byte) bool {

	return (findOffset(buffer) == 0)
}

func AllocateAlignedBuffer() []byte {

	buffer := make([]byte, (8192))

	if isAligned(buffer) {
		return buffer[:4096]
	}

	// distance from previous aligned address just smaller than the starting address of buffer.
	offset := findOffset(buffer)

	// distance to next page aligned address just greater than starting address of buffer.
	distance := 4096 - offset

	return buffer[distance : distance+4096]

}
