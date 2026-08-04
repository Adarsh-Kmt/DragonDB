package replication

import (
	"sync"
)

type ReplicationManager struct {
	mutex                 *sync.Mutex
	replicationChannelMap map[uint64]chan ReplicationUnit
}

func (manager ReplicationManager) GetReplicationChannel(bPlusTreeId uint64) chan<- ReplicationUnit {

	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	if channel, exists := manager.replicationChannelMap[bPlusTreeId]; exists {

		return channel
	}

	channel := make(chan ReplicationUnit, 1000)
	manager.replicationChannelMap[bPlusTreeId] = channel

	return channel
}
