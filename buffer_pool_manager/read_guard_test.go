package buffer_pool_manager

import "github.com/stretchr/testify/suite"

type ReadGuardTestSuite struct {
	suite.Suite
	bufferPool SimpleBufferPoolManager
}

func (rs *ReadGuardTestSuite) SetupTest() {

	replacer := NewLRUReplacer()
	disk, err := NewDiskManager("/test")

	rs.Suite.Assert().NoError(err)
	bpm := NewSimpleBufferPoolManager(5, replacer, disk)

	rs.bufferPool = *bpm

}

func (rs *ReadGuardTestSuite) TearDownTest() {
	rs.Suite.Assert().NoError(rs.bufferPool.Close())
}

func (rs *ReadGuardTestSuite) TestReadGuardDone() {

	guard, err := rs.bufferPool.NewReadGuard(1)

	rs.Suite.Assert().NoError(err)

	ok := guard.Done()

	rs.Suite.Assert().Equal(true, ok)

	ok = guard.Done()

	rs.Suite.Assert().Equal(false, ok)
}
