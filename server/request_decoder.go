package server

import (
	"encoding/binary"
	"fmt"
	"net"
)

func readNBytes(conn net.Conn, N int) ([]byte, error) {

	data := make([]byte, N)

	n, err := conn.Read(data)

	if err != nil {
		return nil, err
	}

	if n != N {
		return nil, fmt.Errorf("incomplete read error")
	}

	return data, nil
}
func decodeInsertRequest(conn net.Conn) (key []byte, value []byte, err error) {

	keyLengthBytes, err := readNBytes(conn, 4)

	if err != nil {
		return nil, nil, err
	}

	keyLength := binary.LittleEndian.Uint32(keyLengthBytes)

	key, err = readNBytes(conn, int(keyLength))

	if err != nil {

		return nil, nil, err
	}

	valueLengthBytes, err := readNBytes(conn, 4)

	if err != nil {
		return nil, nil, err
	}

	valueLength := binary.LittleEndian.Uint32(valueLengthBytes)

	value, err = readNBytes(conn, int(valueLength))

	if err != nil {
		return nil, nil, err
	}

	return key, value, nil

}

func decodeGetRequest(conn net.Conn) (key []byte, err error) {

	keyLengthBytes, err := readNBytes(conn, 4)

	if err != nil {
		return nil, err
	}

	keyLength := binary.LittleEndian.Uint32(keyLengthBytes)

	key, err = readNBytes(conn, int(keyLength))

	if err != nil {

		return nil, err
	}

	return key, nil
}

func decodeDeleteRequest(conn net.Conn) (key []byte, err error) {

	keyLengthBytes, err := readNBytes(conn, 4)

	if err != nil {
		return nil, err
	}

	keyLength := binary.LittleEndian.Uint32(keyLengthBytes)

	key, err = readNBytes(conn, int(keyLength))

	if err != nil {

		return nil, err
	}

	return key, nil

}
