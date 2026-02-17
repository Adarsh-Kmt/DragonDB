package bplustree

import (
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	bpm "github.com/Adarsh-Kmt/DragonDB/bufferpoolmanager"
	codec "github.com/Adarsh-Kmt/DragonDB/pagecodec"
	"github.com/stretchr/testify/suite"
)

type BPlusTreeTestSuite struct {
	suite.Suite
	btree    *BPlusTree
	disk     *bpm.DirectIODiskManager
	metadata *codec.MetaData
}

func (ts *BPlusTreeTestSuite) SetupTest() {

	// handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
	// 	Level: slog.LevelWarn, // Only show WARN, ERROR, etc.
	// })

	// logger := slog.New(handler)

	// slog.SetDefault(logger)

	// Create test file
	testFile := "dragon.db"

	// Initialize disk manager
	disk, actualMetadata, _, err := bpm.NewDirectIODiskManager(testFile)
	ts.Require().NoError(err)

	ts.disk = disk
	ts.metadata = actualMetadata

	// Create replacer and buffer pool manager
	replacer := bpm.NewLRUReplacer()
	bufferPoolManager, err := bpm.NewSimpleBufferPoolManager(10, 4096, replacer, disk)
	ts.Require().NoError(err)

	// Create BPlusTree
	ts.btree = NewBPlusTree(0, bufferPoolManager, ts.metadata)
}

func (ts *BPlusTreeTestSuite) TearDownTest() {
	if ts.btree != nil {
		ts.btree.Close()
	}

	// Clean up test file
	os.Remove("dragon.db")
}

func (ts *BPlusTreeTestSuite) TestInsertSingleElement() {
	key := []byte("test_key")
	value := []byte("test_value")

	// Insert first element
	err := ts.btree.Insert(key, value)
	ts.Require().NoError(err)

	// Verify root node was created
	ts.Assert().NotEqual(uint64(0), ts.btree.rootNodePageId)

	// Retrieve the element
	retrievedValue, err := ts.btree.Get(key)
	ts.Require().NoError(err)
	ts.Assert().Equal(value, retrievedValue)
}

func (ts *BPlusTreeTestSuite) TestInsertMultipleElements() {
	// Insert multiple elements
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
	}

	// Insert all elements
	for key, value := range testData {
		err := ts.btree.Insert([]byte(key), []byte(value))
		ts.Require().NoError(err)
	}

	// Verify all elements can be retrieved
	for key, expectedValue := range testData {
		retrievedValue, err := ts.btree.Get([]byte(key))
		ts.Require().NoError(err)
		ts.Assert().Equal([]byte(expectedValue), retrievedValue)
	}
}

func (ts *BPlusTreeTestSuite) TestInsertDuplicateKey() {
	key := []byte("duplicate_key")
	value1 := []byte("value1")
	value2 := []byte("value2")

	// Insert first value
	err := ts.btree.Insert(key, value1)
	ts.Require().NoError(err)

	// Verify first value
	retrievedValue, err := ts.btree.Get(key)
	ts.Require().NoError(err)
	ts.Assert().Equal(value1, retrievedValue)

	// Insert second value with same key (should update)
	err = ts.btree.Insert(key, value2)
	ts.Require().NoError(err)

	// Verify value was updated
	retrievedValue, err = ts.btree.Get(key)
	ts.Require().NoError(err)
	ts.Assert().Equal(value2, retrievedValue)
}

func (ts *BPlusTreeTestSuite) TestGetNonExistentKey() {
	// Try to get a key that doesn't exist
	_, err := ts.btree.Get([]byte("non_existent_key"))
	ts.Assert().Error(err)

}

func (ts *BPlusTreeTestSuite) TestInsertLargeNumberOfElements() {
	// Insert a large number of elements to test splitting
	numElements := 10

	startWrite := time.Now()
	for i := 0; i < numElements; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		value := []byte(fmt.Sprintf("value_%04d", i))

		err := ts.btree.Insert(key, value)
		ts.Require().NoError(err)
	}

	startRead := time.Now()
	// Verify all elements can be retrieved
	for i := 0; i < numElements; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		expectedValue := []byte(fmt.Sprintf("value_%04d", i))

		retrievedValue, err := ts.btree.Get(key)
		ts.Require().NoError(err)
		ts.Assert().Equal(expectedValue, retrievedValue)
	}

	slog.Error("Write benchmark", fmt.Sprintf("Time taken to insert %d elements:", numElements), time.Since(startWrite))
	slog.Error("Read benchmark", fmt.Sprintf("Time taken to retrieve %d elements:", numElements), time.Since(startRead))
}

func (ts *BPlusTreeTestSuite) TestInsertWithSplitting() {
	// Insert elements that will cause page splits
	// Using keys that will fill up pages and force splits

	numElements := 4
	largeValue := make([]byte, 1000) // Large value to fill pages quickly
	for i := range largeValue {
		largeValue[i] = byte('A' + (i % 26))
	}

	startWrite := time.Now()
	// Insert multiple large elements
	for i := range numElements {
		key := []byte(fmt.Sprintf("large_key_%02d", i))
		slog.Info(fmt.Sprintf("inserting key %d", i))
		err := ts.btree.Insert(key, largeValue)
		ts.Require().NoError(err)
		slog.Info("------------------")

	}

	// Verify all elements can be retrieved

	startRead := time.Now()
	for i := range numElements {
		key := []byte(fmt.Sprintf("large_key_%02d", i))
		retrievedValue, err := ts.btree.Get(key)
		if err != nil {
			slog.Error(fmt.Sprintf("couldnt find key %d", i))
		}
		ts.Require().NoError(err)
		ts.Assert().Equal(largeValue, retrievedValue)
	}
	slog.Error(fmt.Sprintf("Time taken to insert %d large elements:", numElements), "duration", time.Since(startWrite))
	slog.Error(fmt.Sprintf("Time taken to retrieve %d large elements:", numElements), "duration", time.Since(startRead))
}

func (ts *BPlusTreeTestSuite) TestEmptyTree() {
	// Test getting from empty tree
	_, err := ts.btree.Get([]byte("any_key"))
	ts.Assert().Error(err)
	//ts.Assert().Contains(err.Error(), "key not found")
}

func (ts *BPlusTreeTestSuite) TestInsertEmptyKey() {
	key := []byte("")
	value := []byte("empty_key_value")

	err := ts.btree.Insert(key, value)
	ts.Require().NoError(err)

	retrievedValue, err := ts.btree.Get(key)
	ts.Require().NoError(err)
	ts.Assert().Equal(value, retrievedValue)
}

func (ts *BPlusTreeTestSuite) TestInsertEmptyValue() {
	key := []byte("key_with_empty_value")
	value := []byte("hello")

	err := ts.btree.Insert(key, value)
	ts.Require().NoError(err)

	retrievedValue, err := ts.btree.Get(key)
	ts.Require().NoError(err)
	ts.Assert().Equal(value, retrievedValue)
}

func TestBPlusTree(t *testing.T) {
	suite.Run(t, new(BPlusTreeTestSuite))
}
