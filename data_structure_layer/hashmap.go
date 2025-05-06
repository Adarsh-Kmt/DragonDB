package data_structure_layer

import (
	"encoding/binary"
	"fmt"
	"sync"
)

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

	hm.mutex.RLock()
	value = hm.store[k]
	hm.mutex.RUnlock()

	return value, nil
}

func (hm *HashMap) Insert(key []byte, value []byte) error {

	k := binary.LittleEndian.Uint16(key)

	hm.mutex.Lock()
	hm.store[k] = value
	hm.mutex.Unlock()

	return nil
}

func (hm *HashMap) Delete(key []byte) error {

	k := binary.LittleEndian.Uint16(key)

	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	_, exists := hm.store[k]

	if !exists {
		return fmt.Errorf("key not found")
	}

	delete(hm.store, k)

	return nil
}
