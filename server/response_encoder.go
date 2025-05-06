package server

import "encoding/binary"

func encodeGetResponse(key []byte, value []byte) []byte {

	responseLength := 1 + 4 + len(key) + 4 + len(value)

	response := make([]byte, responseLength)

	pointer := 0
	response[pointer] = byte('O')

	pointer += 1

	binary.LittleEndian.PutUint32(response[pointer:pointer+4], uint32(len(key)))
	pointer += 4

	copy(response[pointer:], key)

	pointer += len(key)

	binary.LittleEndian.PutUint32(response[pointer:pointer+4], uint32(len(value)))
	pointer += 4

	copy(response[pointer:], value)

	return response
}

func encodeDeleteResponse() []byte {

	response := make([]byte, 1)

	response[0] = byte('O')

	return response
}

func encodeInsertResponse() []byte {

	response := make([]byte, 1)

	response[0] = byte('O')

	return response
}

func encodeErrorResponse(err error) []byte {

	message := []byte(err.Error())

	responseLength := 1 + 4 + len(message)

	response := make([]byte, responseLength)

	pointer := 0
	response[pointer] = byte('E')

	pointer++

	binary.LittleEndian.PutUint32(response[pointer:pointer+4], uint32(len(message)))
	pointer += 4

	copy(response[pointer:], message)

	return response
}
