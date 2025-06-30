package buffer_pool_manager

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
)

type DirectIODiskManagerTestSuite struct {
	suite.Suite
	diskManager *DirectIODiskManager
}

func setupPage() []byte {

	page := make([]byte, 4096)

	pointer := 0

	for i := range 512 {
		binary.LittleEndian.PutUint64(page[pointer:pointer+8], uint64(i))
		pointer += 8
	}

	return page
}

func setupFile(filePath string) {

	f, _ := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)

	f.Write(setupPage())

	f.Close()
}
func (ds *DirectIODiskManagerTestSuite) SetupSuite() {

	setupFile("test_file")
	diskManager, _, err := NewDirectIODiskManager("test_file")
	ds.Suite.Assert().NoError(err)
	ds.diskManager = diskManager
}

func (ds *DirectIODiskManagerTestSuite) TearDownSuite() {
	err := ds.diskManager.file.Close()
	ds.Assert().NoError(err)
	err = os.Remove("test_file")
	ds.Assert().NoError(err)

}

func (ds *DirectIODiskManagerTestSuite) TestDiskManagerWrite() {

	data := setupPage()
	err := ds.diskManager.write(4096, data)
	ds.Assert().NoError(err)
}

func (ds *DirectIODiskManagerTestSuite) TestDiskManagerRead() {
	data, err := ds.diskManager.read(0, 4096)
	ds.Assert().NoError(err)

	pointer := 0

	for i := range 512 {

		ds.Assert().Equal(uint64(i), binary.LittleEndian.Uint64(data[pointer:pointer+8]))
		pointer += 8
	}

}
func TestDiskManager(t *testing.T) {
	suite.Run(t, new(DirectIODiskManagerTestSuite))
}
