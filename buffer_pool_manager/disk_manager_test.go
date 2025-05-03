package buffer_pool_manager

import (
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
)

type DirectIODiskManagerTestSuite struct {
	suite.Suite
	diskManager *DirectIODiskManager
}

func (ds *DirectIODiskManagerTestSuite) SetupSuite() {
	diskManager, err := NewDirectIODiskManager("test_file.txt")
	ds.Suite.Assert().NoError(err)
	_, err = diskManager.file.Write([]byte("testing disk manager..."))
	ds.Suite.Assert().NoError(err)
	ds.diskManager = diskManager
}

func (ds *DirectIODiskManagerTestSuite) TearDownSuite() {
	err := ds.diskManager.file.Close()
	ds.Assert().NoError(err)
	err = os.Remove("test_file.txt")
	ds.Assert().NoError(err)

}

func (ds *DirectIODiskManagerTestSuite) TestDiskManagerWrite() {

	data := []byte("testing disk manager...")
	err := ds.diskManager.write(int64(len(data)), data)
	ds.Assert().NoError(err)
}

func (ds *DirectIODiskManagerTestSuite) TestDiskManagerRead() {
	data, err := ds.diskManager.read(0, len([]byte("testing disk manager...")))
	ds.Assert().NoError(err)
	ds.Assert().Equal("testing disk manager...", string(data))
}
func TestDiskManager(t *testing.T) {
	suite.Run(t, new(DirectIODiskManagerTestSuite))
}
