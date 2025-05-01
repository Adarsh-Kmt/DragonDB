package buffer_pool_manager

import (
	"encoding/binary"
	"log"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"
)

type BufferPoolManagerTestSuite struct {
	suite.Suite
	bufferPool *SimpleBufferPoolManager
}

func diskManagerSetup(disk *DiskManager, path string) error {

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return err
	}

	pointer := 0

	data := make([]byte, 8*2)

	for i := 0; i < 8; i++ {

		binary.LittleEndian.PutUint16(data[pointer:pointer+2], uint16(i))
		pointer += 2
	}
	log.Printf("file => %v", data)
	if _, err := f.Write(data); err != nil {
		return err
	}

	disk.file = f

	return nil
}
func (bs *BufferPoolManagerTestSuite) SetupTest() {

	disk := &DiskManager{
		mutex:                 &sync.Mutex{},
		deallocatedPageIdList: make([]PageID, 0),
		maxAllocatedPageId:    7,
	}

	err := diskManagerSetup(disk, "test_file.dat")

	bs.Suite.Assert().NoError(err)

	replacer := NewLRUReplacer()

	bs.bufferPool = NewSimpleBufferPoolManager(3, 2, replacer, disk)

}

func (bs *BufferPoolManagerTestSuite) TearDownTest() {

	err := bs.bufferPool.disk.file.Close()
	bs.Suite.Assert().NoError(err)

	err = os.Remove("test_file.dat")

	bs.Suite.Assert().NoError(err)

}

func (bs *BufferPoolManagerTestSuite) TesthMultiplePageFetch() {

	log.Println()

	//------------------------------------------
	// fetch page 1

	log.Println("fetching page 1...")
	frame, err := bs.bufferPool.fetchPage(1)

	bs.Suite.Assert().NoError(err)

	num := binary.LittleEndian.Uint16(frame.data[:2])

	log.Printf("page data => %d", num)

	bs.Suite.Assert().Equal(uint16(1), num)

	log.Printf("page table => %v", bs.bufferPool.pageTable)
	log.Printf("free frames => %v", bs.bufferPool.freeFrames)

	bs.Suite.Assert().Equal(FrameID(1), bs.bufferPool.freeFrames[0])

	//------------------------------------------

	log.Println()

	//------------------------------------------
	// fetch page 0
	log.Println("fetching page 0...")
	frame, err = bs.bufferPool.fetchPage(0)

	bs.Suite.Assert().NoError(err)

	num = binary.LittleEndian.Uint16(frame.data[:2])

	log.Printf("page data => %d", num)

	bs.Suite.Assert().Equal(uint16(0), num)

	log.Printf("page table => %v", bs.bufferPool.pageTable)
	log.Printf("free frames => %v", bs.bufferPool.freeFrames)

	//bs.Suite.Assert().Equal(0, len(bs.bufferPool.freeFrames))

	// unpin page 0
	bs.bufferPool.unpinPage(0)

	bs.Suite.Require().Equal(0, frame.pinCount)

	//------------------------------------------

	log.Println()

	//------------------------------------------
	// fetch page 5

	log.Println("fetching page 5...")

	frame, err = bs.bufferPool.fetchPage(5)

	bs.Suite.Assert().NoError(err)

	num = binary.LittleEndian.Uint16(frame.data[:2])

	log.Printf("page data => %d", num)

	// assert page data
	bs.Suite.Assert().Equal(uint16(5), num)

	log.Printf("page table => %v", bs.bufferPool.pageTable)
	log.Printf("free frames => %v", bs.bufferPool.freeFrames)

	bs.Suite.Assert().Equal(0, len(bs.bufferPool.freeFrames))

	// unpin page 5
	bs.bufferPool.unpinPage(5)

	bs.Suite.Require().Equal(0, frame.pinCount)

	//------------------------------------------

	log.Println()

	//------------------------------------------
	// fetch page 6

	log.Println("fetching page 7...")
	frame, err = bs.bufferPool.fetchPage(7)

	bs.Suite.Assert().NoError(err)

	num = binary.LittleEndian.Uint16(frame.data[:2])

	log.Printf("page data => %d", num)

	bs.Suite.Assert().Equal(uint16(7), num)

	log.Printf("page table => %v", bs.bufferPool.pageTable)
	log.Printf("free frames => %v", bs.bufferPool.freeFrames)

	bs.Suite.Assert().Equal(0, len(bs.bufferPool.freeFrames))

	//------------------------------------------

	log.Println()
}

func (bs *BufferPoolManagerTestSuite) TestUnpinPage() {

	// test unpin without fetch
	result := bs.bufferPool.unpinPage(0)

	bs.Suite.Assert().Equal(false, result)

	// test unpin after fetching
	frame, err := bs.bufferPool.fetchPage(0)

	bs.Suite.Assert().NoError(err)

	bs.bufferPool.unpinPage(0)

	bs.Suite.Assert().Equal(0, frame.pinCount)

}

func (bs *BufferPoolManagerTestSuite) TestDeletePage() {

	// test delete without fetch

	result, err := bs.bufferPool.deletePage(0)

	bs.Suite.Assert().NoError(err)

	bs.Suite.Assert().Equal(false, result)

	_, err = bs.bufferPool.fetchPage(0)

	bs.Suite.Assert().NoError(err)

	log.Printf("page table => %v", bs.bufferPool.pageTable)
	log.Printf("free frames => %v", bs.bufferPool.freeFrames)

	_, err = bs.bufferPool.deletePage(0)

	bs.Suite.Assert().NoError(err)
	log.Printf("page table => %v", bs.bufferPool.pageTable)
	log.Printf("deallocated page id list => %v", bs.bufferPool.disk.deallocatedPageIdList)
	bs.Suite.Assert().Equal(PageID(0), bs.bufferPool.disk.deallocatedPageIdList[0])

}

func (bs *BufferPoolManagerTestSuite) TestNewPage() {

	// should return max allocated page ID
	pageId := bs.bufferPool.NewPage()

	bs.Suite.Assert().Equal(PageID(8), pageId)

	// delete page 0, check if new page returns 0
	_, err := bs.bufferPool.fetchPage(0)

	bs.Suite.Require().NoError(err)

	result, err := bs.bufferPool.deletePage(0)

	bs.Suite.Assert().NoError(err)

	bs.Suite.Assert().Equal(true, result)

	pageId = bs.bufferPool.NewPage()

	bs.Suite.Assert().Equal(PageID(0), pageId)
}

func (bs *BufferPoolManagerTestSuite) TestDirtyPageEviction() {

	// fetch page 0 from disk.
	frame, err := bs.bufferPool.fetchPage(0)

	bs.Suite.Require().NoError(err)

	// update page 0
	binary.LittleEndian.PutUint16(frame.data[:2], uint16(10))

	frame.dirty = true

	bs.bufferPool.unpinPage(0)

	// evict page 0 by fetching other pages.
	bs.bufferPool.fetchPage(5)
	bs.bufferPool.fetchPage(3)
	bs.bufferPool.fetchPage(2)

	bs.bufferPool.unpinPage(3)

	// fetch page 0 from disk again, check if updated.
	frame, err = bs.bufferPool.fetchPage(0)

	bs.Suite.Require().NoError(err)

	bs.Suite.Assert().Equal(uint16(10), binary.LittleEndian.Uint16(frame.data[:2]))

}
func TestBufferPoolManager(t *testing.T) {

	suite.Run(t, new(BufferPoolManagerTestSuite))
}
