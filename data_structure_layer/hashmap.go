package data_structure_layer

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
)
// dummy data structure layer to test the database server, because I'm too lazy to build a B-Tree from scratch rn.
type HashMap struct {
	store map[uint16][]byte
	mutex *sync.RWMutex
}

func NewHashMap() *HashMap {

	return &HashMap{
		store: map[uint16][]byte{},
		mutex: &sync.RWMutex{},
	}
}
func (hm *HashMap) Get(key []byte) (value []byte, err error) {

	k := binary.LittleEndian.Uint16(key)

	slog.Info(fmt.Sprintf("getting key = %d", k))
	hm.mutex.RLock()
	value, exists := hm.store[k]
	hm.mutex.RUnlock()

	if !exists {
		return nil, fmt.Errorf("key not found")
	}

	return value, nil
}

func (hm *HashMap) Insert(key []byte, value []byte) error {

	k := binary.LittleEndian.Uint16(key)

	slog.Info(fmt.Sprintf("inserting key = %d value = %s", k, string(value)))
	hm.mutex.Lock()
	hm.store[k] = value
	hm.mutex.Unlock()

	return nil
}

func (hm *HashMap) Delete(key []byte) error {

	k := binary.LittleEndian.Uint16(key)

	slog.Info(fmt.Sprintf("deleting key = %d", k))
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	_, exists := hm.store[k]

	if !exists {
		return fmt.Errorf("key not found")
	}

	delete(hm.store, k)

	return nil
}

func (hm *HashMap) Close() error {
	return nil
}
