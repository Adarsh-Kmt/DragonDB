package data_structure_layer

import (
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/Adarsh-Kmt/DragonDB/buffer_pool_manager"
	codec "github.com/Adarsh-Kmt/DragonDB/page_codec"
	"github.com/stretchr/testify/suite"
)

type BTreeTestSuite struct {
	suite.Suite
	btree    *BTree
	disk     *buffer_pool_manager.DirectIODiskManager
	metadata *codec.MetaData
}

func (ts *BTreeTestSuite) SetupTest() {

	// handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
	// 	Level: slog.LevelWarn, // Only show WARN, ERROR, etc.
	// })

	// logger := slog.New(handler)

	// slog.SetDefault(logger)

	// Create test file
	testFile := "dragon.db"

	// Initialize disk manager
	disk, actualMetadata, err := buffer_pool_manager.NewDirectIODiskManager(testFile)
	ts.Require().NoError(err)

	ts.disk = disk
	ts.metadata = actualMetadata

	// Create replacer and buffer pool manager
	replacer := buffer_pool_manager.NewLRUReplacer()
	bufferPoolManager, err := buffer_pool_manager.NewSimpleBufferPoolManager(10, 4096, replacer, disk)
	ts.Require().NoError(err)

	// Create BTree
	ts.btree = NewBTree(bufferPoolManager, ts.metadata)
}

func (ts *BTreeTestSuite) TearDownTest() {
	if ts.btree != nil {
		ts.btree.Close()
	}

	// Clean up test file
	os.Remove("dragon.db")
}

func (ts *BTreeTestSuite) TestInsertSingleElement() {
	key := []byte("test_key")
	value := []byte("test_value")

	// Insert first element
	err := ts.btree.Insert(key, value)
	ts.Require().NoError(err)

	// Verify root node was created
	ts.Assert().NotEqual(uint64(0), ts.metadata.RootNodePageId)

	// Retrieve the element
	retrievedValue, err := ts.btree.Get(key)
	ts.Require().NoError(err)
	ts.Assert().Equal(value, retrievedValue)
}

func (ts *BTreeTestSuite) TestInsertMultipleElements() {
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

func (ts *BTreeTestSuite) TestInsertDuplicateKey() {
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

func (ts *BTreeTestSuite) TestGetNonExistentKey() {
	// Try to get a key that doesn't exist
	_, err := ts.btree.Get([]byte("non_existent_key"))
	ts.Assert().Error(err)

}

func (ts *BTreeTestSuite) TestInsertLargeNumberOfElements() {
	// Insert a large number of elements to test splitting
	numElements := 500

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

func (ts *BTreeTestSuite) TestInsertWithSplitting() {
	// Insert elements that will cause page splits
	// Using keys that will fill up pages and force splits

	numElements := 10
	largeValue := make([]byte, 1000) // Large value to fill pages quickly
	for i := range largeValue {
		largeValue[i] = byte('A' + (i % 26))
	}

	startWrite := time.Now()
	// Insert multiple large elements
	for i := range numElements {
		key := []byte(fmt.Sprintf("large_key_%02d", i))
		err := ts.btree.Insert(key, largeValue)
		ts.Require().NoError(err)
	}

	// Verify all elements can be retrieved

	startRead := time.Now()
	for i := range numElements {
		key := []byte(fmt.Sprintf("large_key_%02d", i))
		retrievedValue, err := ts.btree.Get(key)
		ts.Require().NoError(err)
		ts.Assert().Equal(largeValue, retrievedValue)
	}
	slog.Error(fmt.Sprintf("Time taken to insert %d large elements:", numElements), "duration", time.Since(startWrite))
	slog.Error(fmt.Sprintf("Time taken to retrieve %d large elements:", numElements), "duration", time.Since(startRead))
}

func (ts *BTreeTestSuite) TestEmptyTree() {
	// Test getting from empty tree
	_, err := ts.btree.Get([]byte("any_key"))
	ts.Assert().Error(err)
	//ts.Assert().Contains(err.Error(), "key not found")
}

func (ts *BTreeTestSuite) TestInsertEmptyKey() {
	key := []byte("")
	value := []byte("empty_key_value")

	err := ts.btree.Insert(key, value)
	ts.Require().NoError(err)

	retrievedValue, err := ts.btree.Get(key)
	ts.Require().NoError(err)
	ts.Assert().Equal(value, retrievedValue)
}

func (ts *BTreeTestSuite) TestInsertEmptyValue() {
	key := []byte("key_with_empty_value")
	value := []byte("hello")

	err := ts.btree.Insert(key, value)
	ts.Require().NoError(err)

	retrievedValue, err := ts.btree.Get(key)
	ts.Require().NoError(err)
	ts.Assert().Equal(value, retrievedValue)
}

func TestBTree(t *testing.T) {
	suite.Run(t, new(BTreeTestSuite))
}
