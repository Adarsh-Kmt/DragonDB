package buffer_pool_manager

import "github.com/stretchr/testify/suite"

type WriteGuardTestSuite struct {
	suite.Suite
	bufferPool SimpleBufferPoolManager
}

func (ws *WriteGuardTestSuite) SetupTest() {

	replacer := NewLRUReplacer()
	disk, err := NewOSBufferedDiskManager("/test")

	ws.Suite.Assert().NoError(err)
	bpm := NewSimpleBufferPoolManager(5, 4096, replacer, disk)

	ws.bufferPool = *bpm

}

func (ws *WriteGuardTestSuite) TearDownTest() {
	ws.Suite.Assert().NoError(ws.bufferPool.Close())
}

func (ws *WriteGuardTestSuite) TestWriteGuardDone() {

	guard, err := ws.bufferPool.NewReadGuard(1)

	ws.Suite.Assert().NoError(err)

	ok := guard.Done()

	ws.Suite.Assert().Equal(true, ok)

	ok = guard.Done()

	ws.Suite.Assert().Equal(false, ok)
}
