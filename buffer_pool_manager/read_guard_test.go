package buffer_pool_manager

import "github.com/stretchr/testify/suite"

type ReadGuardTestSuite struct {
	suite.Suite
	bufferPool SimpleBufferPoolManager
}

func (rs *ReadGuardTestSuite) SetupTest() {

	replacer := NewLRUReplacer()
	disk, err := NewOSBufferedDiskManager("/test")

	rs.Suite.Assert().NoError(err)
	bpm, err := NewSimpleBufferPoolManager(5, 4096, replacer, disk)

	rs.Suite.Assert().NoError(err)
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
