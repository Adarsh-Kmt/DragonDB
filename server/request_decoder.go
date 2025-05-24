package server

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type Request struct {
	opCode string
	body   []byte
}

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

func readUInt32(reader io.Reader) (uint32, error) {

	data, err := readNBytes(reader, 4)

	if err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint32(data), nil
}

func readRequest(conn net.Conn) (*Request, error) {

	opCodeByte, err := readNBytes(conn, 1)

	if err != nil {
		return nil, err
	}

	opCode := string(opCodeByte)

	request := &Request{}

	request.opCode = opCode

	if request.opCode == "P" || request.opCode == "S" || request.opCode == "C" {
		return request, nil
	}

	requestBodyLength, err := readUInt32(conn)

	if err != nil {
		return nil, err
	}

	requestBody, err := readNBytes(conn, int(requestBodyLength))

	if err != nil {
		return nil, err
	}

	request.body = requestBody

	return request, nil
}
func decodeInsertRequestBody(body []byte) (key []byte, value []byte) {

	pointer := 0
	keyLength := binary.LittleEndian.Uint32(body[pointer : pointer+4])

	pointer += 4

	key = make([]byte, keyLength)

	copy(key, body[pointer:pointer+int(keyLength)])

	pointer += int(keyLength)

	valueLength := binary.LittleEndian.Uint32(body[pointer : pointer+4])
	pointer += 4

	value = make([]byte, valueLength)

	copy(value, body[pointer:pointer+int(valueLength)])

	return key, value

}

func decodeGetRequestBody(body []byte) (key []byte) {

	pointer := 0
	keyLength := binary.LittleEndian.Uint32(body[pointer : pointer+4])

	pointer += 4

	key = make([]byte, keyLength)

	copy(key, body[pointer:pointer+int(keyLength)])

	return key
}

func decodeDeleteRequestBody(body []byte) (key []byte) {

	pointer := 0
	keyLength := binary.LittleEndian.Uint32(body[pointer : pointer+4])

	pointer += 4

	key = make([]byte, keyLength)

	copy(key, body[pointer:pointer+int(keyLength)])

	return key

}
