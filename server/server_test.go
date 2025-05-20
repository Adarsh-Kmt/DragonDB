package server

import (
	"encoding/binary"
	"log/slog"
	"net"
	"testing"

	"github.com/Adarsh-Kmt/DragonDB/data_structure_layer"
	"github.com/stretchr/testify/suite"
)

type DatabaseServerTestSuite struct {
	suite.Suite
	server *Server
	conn   net.Conn
}

func (test *DatabaseServerTestSuite) SetupTest() {

	server, err := NewServer(":8080", data_structure_layer.NewHashMap())

	test.Suite.Require().NoError(err)

	test.server = server

	serverAddr, err := net.ResolveTCPAddr("tcp", "localhost:8080")

	test.Suite.Require().NoError(err)

	go server.Run()

	conn, err := net.DialTCP("tcp", nil, serverAddr)

	test.conn = conn

	test.Suite.Require().NoError(err)

}

func (test *DatabaseServerTestSuite) TearDownTest() {

	test.server.Shutdown()

	shutdownMessage := make([]byte, 1)

	n, err := test.conn.Read(shutdownMessage)

	test.Suite.Require().NoError(err)

	test.Suite.Require().Equal(1, n)
	test.Suite.Require().Equal("S", string(shutdownMessage[0]))
	slog.Info("received shutdown message")

	test.conn.Close()
}

func createInsertRequest(key uint16, value []byte) []byte {

	request := make([]byte, 1+4+2+4+len(value))

	pointer := 0

	request[pointer] = byte('I')
	pointer += 1

	binary.LittleEndian.PutUint32(request[pointer:pointer+4], uint32(2))
	pointer += 4

	binary.LittleEndian.PutUint16(request[pointer:pointer+2], key)
	pointer += 2

	binary.LittleEndian.PutUint32(request[pointer:pointer+4], uint32(len(value)))
	pointer += 4

	copy(request[pointer:pointer+len(value)], value)

	return request
}

func createGetRequest(key uint16) []byte {

	request := make([]byte, 1+4+2)

	pointer := 0

	request[pointer] = byte('G')
	pointer += 1

	binary.LittleEndian.PutUint32(request[pointer:pointer+4], uint32(2))
	pointer += 4

	binary.LittleEndian.PutUint16(request[pointer:pointer+2], key)

	return request

}
func (test *DatabaseServerTestSuite) TestInsert() {

	request := createInsertRequest(5, []byte("hello"))

	n, err := test.conn.Write(request)

	test.Suite.Require().NoError(err)
	test.Suite.Require().Equal(len(request), n)

	responseOpCode := make([]byte, 1)

	n, err = test.conn.Read(responseOpCode)

	test.Suite.Require().NoError(err)
	test.Suite.Require().Equal(1, n)

	test.Suite.Require().Equal("O", string(responseOpCode[0]))
}

func (test *DatabaseServerTestSuite) TestGet() {

	request := createInsertRequest(5, []byte("hello"))

	n, err := test.conn.Write(request)

	test.Suite.Require().NoError(err)
	test.Suite.Require().Equal(len(request), n)

	responseOpCode := make([]byte, 1)

	n, err = test.conn.Read(responseOpCode)

	test.Suite.Require().NoError(err)
	test.Suite.Require().Equal(1, n)

	test.Suite.Require().Equal("O", string(responseOpCode[0]))

	request = createGetRequest(5)

	n, err = test.conn.Write(request)

	test.Suite.Require().NoError(err)
	test.Suite.Require().Equal(len(request), n)

	responseOpCode, err = readNBytes(test.conn, 1)
	test.Suite.Require().NoError(err)

	test.Suite.Require().Equal("O", string(responseOpCode[0]))

	keyLen, err := readNBytes(test.conn, 4)

	test.Suite.Require().NoError(err)

	key, err := readNBytes(test.conn, int(binary.LittleEndian.Uint32(keyLen)))

	test.Suite.Require().NoError(err)
	test.Suite.Require().Equal(uint16(5), binary.LittleEndian.Uint16(key))

	valueLen, err := readNBytes(test.conn, 4)

	test.Suite.Require().NoError(err)

	value, err := readNBytes(test.conn, int(binary.LittleEndian.Uint32(valueLen)))

	test.Suite.Require().NoError(err)
	test.Suite.Require().Equal([]byte("hello"), value)

}
func TestDatabaseServer(t *testing.T) {

	suite.Run(t, new(DatabaseServerTestSuite))
}
