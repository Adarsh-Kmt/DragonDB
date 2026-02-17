package bufferpoolmanager

import (
	"encoding/binary"
	"log"
	"os"
	"sync"
	"testing"

	codec "github.com/Adarsh-Kmt/DragonDB/pagecodec"
	"github.com/ncw/directio"
	"github.com/stretchr/testify/suite"
)

type BufferPoolManagerTestSuite struct {
	suite.Suite
	bufferPool *SimpleBufferPoolManager
	disk       *DirectIODiskManager
}

func createPage(start int) []byte {

	page := make([]byte, 4096)

	pointer := 0
	for i := 0; i < 512; i++ {
		binary.LittleEndian.PutUint64(page[pointer:pointer+8], uint64(start+i))
		pointer += 8
	}

	return page
}

func checkPage(start int, page []byte) bool {

	pointer := 0

	for i := 0; i < 512; i++ {
		if uint64(i+start) != binary.LittleEndian.Uint64(page[pointer:pointer+8]) {
			return false
		}
		pointer += 8
	}
	return true
}
func fileSetup(path string) error {

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return err
	}

	for i := range 8 {

		page := createPage(i)

		if _, err := f.Write(page); err != nil {
			return err
		}
	}

	return f.Close()

}
func (bs *BufferPoolManagerTestSuite) SetupTest() {

	err := fileSetup("test_file")
	bs.Require().NoError(err)

	replacer := NewLRUReplacer()

	file, err := directio.OpenFile("test_file", os.O_RDWR|os.O_CREATE, 0644)

	bs.Require().NoError(err)

	disk := &DirectIODiskManager{
		file:  file,
		mutex: &sync.Mutex{},
		metadata: &codec.MetaData{
			DeallocatedPageIdList: make([]uint64, 0),
			MaxAllocatedPageId:    7,
		},
	}

	bs.disk = disk

	bs.bufferPool, err = NewSimpleBufferPoolManager(3, 4096, replacer, disk)

	bs.Require().NoError(err)

}

func (bs *BufferPoolManagerTestSuite) TearDownTest() {

	err := bs.disk.file.Close()
	bs.Suite.Assert().NoError(err)

	err = os.Remove("test_file")

	bs.Suite.Assert().NoError(err)

}

func (bs *BufferPoolManagerTestSuite) TesthMultiplePageFetch() {

	log.Println()

	//------------------------------------------
	// fetch page 1

	log.Println("fetching page 1...")
	frame, err := bs.bufferPool.fetchPage(1)

	bs.Suite.Assert().NoError(err)

	bs.Suite.Assert().Equal(true, checkPage(1, frame.data))

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

	bs.Suite.Assert().Equal(true, checkPage(0, frame.data))

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

	bs.Suite.Assert().Equal(true, checkPage(5, frame.data))

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

	bs.Suite.Assert().Equal(true, checkPage(7, frame.data))

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
	log.Printf("deallocated page id list => %v", bs.disk.metadata.DeallocatedPageIdList)
	bs.Suite.Assert().Equal(uint64(0), bs.disk.metadata.DeallocatedPageIdList[0])

}

func (bs *BufferPoolManagerTestSuite) TestNewPage() {

	// should return max allocated page ID
	pageId, err := bs.bufferPool.NewPage()

	bs.Suite.Require().NoError(err)
	bs.Suite.Assert().Equal(uint64(8), pageId)

	// delete page 0, check if new page returns 0
	_, err = bs.bufferPool.fetchPage(0)

	bs.Suite.Require().NoError(err)

	result, err := bs.bufferPool.deletePage(0)

	bs.Suite.Assert().NoError(err)

	bs.Suite.Assert().Equal(true, result)

	pageId, err = bs.bufferPool.NewPage()
	bs.Suite.Require().NoError(err)
	bs.Suite.Assert().Equal(uint64(0), pageId)
}

func (bs *BufferPoolManagerTestSuite) TestDirtyPageEviction() {

	// fetch page 0 from disk.
	frame, err := bs.bufferPool.fetchPage(0)

	bs.Suite.Require().NoError(err)

	// update page 0
	page := createPage(10)

	frame.dirty = true
	frame.data = page

	bs.bufferPool.unpinPage(0)

	// evict page 0 by fetching other pages.
	bs.bufferPool.fetchPage(5)
	bs.bufferPool.fetchPage(3)
	bs.bufferPool.fetchPage(2)

	bs.bufferPool.unpinPage(3)

	// fetch page 0 from disk again, check if updated.
	frame, err = bs.bufferPool.fetchPage(0)

	bs.Suite.Require().NoError(err)

	bs.Suite.Assert().Equal(true, checkPage(10, frame.data))

}
func TestBufferPoolManager(t *testing.T) {

	suite.Run(t, new(BufferPoolManagerTestSuite))
}
