package server

import (
	"encoding/binary"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"testing"

	"github.com/Adarsh-Kmt/DragonDB/buffer_pool_manager"
	"github.com/Adarsh-Kmt/DragonDB/data_structure_layer"
	"github.com/stretchr/testify/suite"
)

type DatabaseServerTestSuite struct {
	suite.Suite
	server *Server
	conn   net.Conn
}

func (test *DatabaseServerTestSuite) SetupTest() {

	testFile := "dragon.db"

	disk, actualMetadata, err := buffer_pool_manager.NewDirectIODiskManager(testFile)

	test.Require().NoError(err)

	replacer := buffer_pool_manager.NewLRUReplacer()
	bufferPoolManager, err := buffer_pool_manager.NewSimpleBufferPoolManager(10, 4096, replacer, disk)
	test.Require().NoError(err)

	server, err := NewServer(":8080", data_structure_layer.NewBTree(bufferPoolManager, actualMetadata))

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

	log.Printf("shutdown message received %v", shutdownMessage)
	test.Suite.Require().NoError(err)

	test.Suite.Require().Equal(1, n)
	test.Suite.Require().Equal("S", string(shutdownMessage[0]))
	slog.Info("received shutdown message")

	test.conn.Close()

	// Clean up test file
	os.Remove("dragon.db")
}

func createInsertRequest(key uint16, value []byte) []byte {

	request := make([]byte, 1+4+4+2+4+len(value))

	pointer := 0

	request[pointer] = byte('I')
	pointer += 1

	requestBodyLength := 4 + 2 + 4 + len(value)

	binary.LittleEndian.PutUint32(request[pointer:pointer+4], uint32(requestBodyLength))
	pointer += 4

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

	request := make([]byte, 1+4+4+2)

	pointer := 0

	request[pointer] = byte('G')
	pointer += 1

	requestBodyLength := 4 + 2

	binary.LittleEndian.PutUint32(request[pointer:pointer+4], uint32(requestBodyLength))
	pointer += 4

	binary.LittleEndian.PutUint32(request[pointer:pointer+4], uint32(2))
	pointer += 4

	binary.LittleEndian.PutUint16(request[pointer:pointer+2], key)

	log.Printf("%v", request)
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

	slog.Info(fmt.Sprintf("response op code for get request %s", string(responseOpCode)))
	test.Suite.Require().NoError(err)

	test.Suite.Require().Equal("O", string(responseOpCode[0]))

	requestBodylength, err := readNBytes(test.conn, 4)

	test.Suite.Require().NoError(err)
	test.Suite.Require().Equal(uint32(15), binary.LittleEndian.Uint32(requestBodylength))
	keyLen, err := readNBytes(test.conn, 4)

	test.Suite.Require().NoError(err)
	test.Suite.Require().Equal(uint32(2), binary.LittleEndian.Uint32(keyLen))
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
