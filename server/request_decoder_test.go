package server

import (
	"encoding/binary"
	"log"
	"testing"

	"github.com/stretchr/testify/suite"
)

type RequestDecoderTestSuite struct {
	suite.Suite
}

func createInsertRequestBody(key []byte, value []byte) []byte {

	request := make([]byte, 4+len(key)+4+len(value))

	pointer := 0

	binary.LittleEndian.PutUint32(request[pointer:pointer+4], uint32(len(key)))

	pointer += 4

	copy(request[pointer:pointer+len(key)], key)

	pointer += len(key)

	binary.LittleEndian.PutUint32(request[pointer:pointer+4], uint32(len(value)))

	pointer += 4

	copy(request[pointer:pointer+len(value)], value)

	log.Printf("%v", request)
	return request
}

func (ts *RequestDecoderTestSuite) TestDecodeInsertRequest() {

	request := createInsertRequestBody([]byte("hello"), []byte("world"))

	key, value := decodeInsertRequestBody(request)

	ts.Suite.Assert().Equal([]byte("hello"), key)

	ts.Suite.Assert().Equal([]byte("world"), value)

}

func createDeleteRequestBody(key []byte) []byte {

	request := make([]byte, 4+len(key))

	pointer := 0

	binary.LittleEndian.PutUint32(request[pointer:pointer+4], uint32(len(key)))

	pointer += 4

	copy(request[pointer:pointer+len(key)], key)

	return request

}
func (ts *RequestDecoderTestSuite) TestDecodeDeleteRequest() {

	request := createDeleteRequestBody([]byte("hello"))

	key := decodeDeleteRequestBody(request)

	ts.Suite.Assert().Equal([]byte("hello"), key)
}

func TestRequestDecoder(t *testing.T) {

	suite.Run(t, new(RequestDecoderTestSuite))
}
