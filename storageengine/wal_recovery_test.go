package storageengine

import (
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRecoverAfterInsert creates a StorageEngine, creates a B+ tree,
// inserts via the B+ tree API, simulates a crash (no engine close),
// runs recovery into a fresh StorageEngine and verifies entries are present.
func TestRecoverAfterInsert(t *testing.T) {
	// create engine and tree, perform inserts
	engine1, _, err := NewStorageEngine()
	require.NoError(t, err)
	defer func() {
		_ = os.Remove("dragon.db")
		_ = os.Remove("lucario.wal")
	}()

	bptId := engine1.NewBPlusTree()
	btree, _ := engine1.OpenBPlusTree(bptId)

	// insert some keys
	require.NoError(t, btree.Insert([]byte("k1"), []byte("v1")))
	require.NoError(t, btree.Insert([]byte("k2"), []byte("v2")))

	engine1.wal.Close()
	// Do NOT close engine1 (simulate crash)

	// create a fresh engine and recover from WAL
	engine2, _, err := NewStorageEngine()
	require.NoError(t, err)

	// run recovery which should replay WAL and populate metadata/pages
	require.NoError(t, engine2.Recover())

	// open same B+ tree on recovered engine and verify keys
	recoveredTree, _ := engine2.OpenBPlusTree(bptId)

	v1, err := recoveredTree.Get([]byte("k1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), v1)

	v2, err := recoveredTree.Get([]byte("k2"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), v2)
}

// TestRecoverAfterSplits inserts enough entries to cause splits then verifies recovery.
func TestRecoverAfterSplits(t *testing.T) {

	// 1. Open the file to write logs to.
	// Use O_APPEND so you don't overwrite previous logs on restart.
	logFile, err := os.OpenFile("./application.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	defer logFile.Close() // Make sure to close the file at the end

	// 2. Create a JSON or Text Handler that writes to the file
	handler := slog.NewJSONHandler(logFile, nil)

	// 3. Create the new logger
	logger := slog.New(handler)

	// 4. Set it as the default global logger (optional)
	slog.SetDefault(logger)

	engine1, _, err := NewStorageEngine()
	require.NoError(t, err)
	defer func() {
		_ = os.Remove("dragon.db")
		_ = os.Remove("lucario.wal")
	}()

	bptId := engine1.NewBPlusTree()
	btree, _ := engine1.OpenBPlusTree(bptId)

	numElements := 20
	largeValue := make([]byte, 1000) // Large value to fill pages quickly
	for i := range largeValue {
		largeValue[i] = byte('A' + (i % 26))
	}

	// Insert multiple large elements
	for i := range numElements {
		key := []byte(fmt.Sprintf("large_key_%02d", i))
		slog.Info(fmt.Sprintf("inserting key %d", i))
		err := btree.Insert(key, largeValue)
		require.NoError(t, err)
		slog.Info("------------------")

	}

	// simulate crash (do not close engine1)

	engine2, _, err := NewStorageEngine()
	require.NoError(t, err)
	defer func() { _ = engine2.wal.Close() }()

	require.NoError(t, engine2.Recover())

	recoveredTree, _ := engine2.OpenBPlusTree(bptId)

	for i := range numElements {
		key := []byte(fmt.Sprintf("large_key_%02d", i))
		retrievedValue, err := recoveredTree.Get(key)
		if err != nil {
			slog.Error(fmt.Sprintf("couldnt find key %d", i))
		}
		assert.NoError(t, err)
		assert.Equal(t, largeValue, retrievedValue)
	}
	// // verify a few sample keys exist after recovery
	// for _, k := range [][]byte{[]byte("key_A"), []byte("key_B"), []byte("key_C")} {
	// 	v, err := recoveredTree.Get(k)
	// 	require.NoError(t, err)
	// 	require.NotNil(t, v)
	// }
}
