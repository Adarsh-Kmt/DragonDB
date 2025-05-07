package server

import (
	"encoding/binary"
	"fmt"
	"io"
)

func readNBytes(reader io.Reader, N int) ([]byte, error) {

	data := make([]byte, N)

	n, err := reader.Read(data)

	if err != nil {
		return nil, err
	}

	if n != N {
		return nil, fmt.Errorf("incomplete read error")
	}

	return data, nil
}
func decodeInsertRequestBody(reader io.Reader) (key []byte, value []byte, err error) {

	keyLengthBytes, err := readNBytes(reader, 4)

	if err != nil {
		return nil, nil, err
	}

	keyLength := binary.LittleEndian.Uint32(keyLengthBytes)

	key, err = readNBytes(reader, int(keyLength))

	if err != nil {

		return nil, nil, err
	}

	valueLengthBytes, err := readNBytes(reader, 4)

	if err != nil {
		return nil, nil, err
	}

	valueLength := binary.LittleEndian.Uint32(valueLengthBytes)

	value, err = readNBytes(reader, int(valueLength))

	if err != nil {
		return nil, nil, err
	}

	return key, value, nil

}

func decodeGetRequestBody(reader io.Reader) (key []byte, err error) {

	keyLengthBytes, err := readNBytes(reader, 4)

	if err != nil {
		return nil, err
	}

	keyLength := binary.LittleEndian.Uint32(keyLengthBytes)

	key, err = readNBytes(reader, int(keyLength))

	if err != nil {

		return nil, err
	}

	return key, nil
}

func decodeDeleteRequestBody(reader io.Reader) (key []byte, err error) {

	keyLengthBytes, err := readNBytes(reader, 4)

	if err != nil {
		return nil, err
	}

	keyLength := binary.LittleEndian.Uint32(keyLengthBytes)

	key, err = readNBytes(reader, int(keyLength))

	if err != nil {

		return nil, err
	}

	return key, nil

}
