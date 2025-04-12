package buffer_pool_manager

import (
	"container/list"
	"testing"

	"github.com/stretchr/testify/suite"
)

type LRUReplacerTestSuite struct {
	suite.Suite
	replacer LRUReplacer
}

func (rs *LRUReplacerTestSuite) SetupTest() {

	l := list.New()
	frame5 := l.PushFront(5)
	frame1 := l.PushFront(1)
	frame4 := l.PushFront(4)
	frame3 := l.PushFront(3)

	frameMap := map[FrameID]*list.Element{}

	frameMap[5] = frame5
	frameMap[1] = frame1
	frameMap[4] = frame4
	frameMap[3] = frame3

	replacer := LRUReplacer{
		frameMap: frameMap,
		list:     l,
	}

	rs.replacer = replacer

}

func (rs *LRUReplacerTestSuite) TearDownTest() {

}

func (rs *LRUReplacerTestSuite) TestLRUReplacerInsert() {

	rs.replacer.insert(2)

	size := rs.replacer.size()

	rs.Suite.Assert().Equal(5, size)

	MRU := rs.replacer.list.Front()

	rs.Suite.Assert().Equal(FrameID(2), MRU.Value.(FrameID))

}

func (rs *LRUReplacerTestSuite) TestLRUReplacerVictim() {

	victim := rs.replacer.victim()

	rs.Suite.Assert().Equal(FrameID(5), victim)
}

func (rs *LRUReplacerTestSuite) TestLRUReplacerRemove() {

	rs.replacer.remove(1)

	_, exists := rs.replacer.frameMap[1]

	rs.Suite.Assert().Equal(false, exists)
}

func TestLRUReplacer(t *testing.T) {

	suite.Run(t, new(LRUReplacerTestSuite))
}
