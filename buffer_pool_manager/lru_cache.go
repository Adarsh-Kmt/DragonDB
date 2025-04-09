package buffer_pool_manager

import (
	"time"
)

type Cache interface {
	RetrievePage(pageId uint64) (data []byte, exists bool)
	CachePage(pageId uint64, data []byte, dirty bool) (dirtyPageId *uint64, dirtyPageData []byte)
	GetDirtyPages() map[uint64][]byte
	PinPage(pageId uint64) bool
	UnpinPage(pageId uint64) bool
}
type LRUCache struct {
	maxSize  int
	currSize int

	LRUPointer *Node
	MRUPointer *Node

	PageNodeMap map[uint64]*Node
}

type Node struct {
	pageId uint64
	data   []byte

	lastAccessed time.Time
	dirty        bool
	pinCount     int

	prev *Node
	next *Node
}

func newNode(pageId uint64, data []byte, dirty bool) *Node {
	return &Node{
		pageId: pageId,
		data:   data,

		prev: nil,
		next: nil,

		lastAccessed: time.Now(),
		dirty:        dirty,
		pinCount:     0,
	}
}
func NewLRUCache(size int) *LRUCache {

	LRUPointer := newNode(0, []byte("LRU"), CLEAN)
	MRUPointer := newNode(0, []byte("MRU"), CLEAN)

	LRUPointer.next = MRUPointer
	LRUPointer.prev = nil

	MRUPointer.prev = LRUPointer
	MRUPointer.next = nil

	return &LRUCache{
		maxSize:     size,
		currSize:    0,
		LRUPointer:  LRUPointer,
		MRUPointer:  MRUPointer,
		PageNodeMap: make(map[uint64]*Node),
	}
}

func (cache *LRUCache) deleteMiddle(node *Node) {

	prevNode := node.prev
	nextNode := node.next

	prevNode.next = nextNode
	nextNode.prev = prevNode

	node.prev = nil
	node.next = nil

}

func (cache *LRUCache) addFront(node *Node) {

	MRUNode := cache.MRUPointer.prev
	MRUNode.next = node
	node.prev = MRUNode

	node.next = cache.MRUPointer
	cache.MRUPointer.prev = node

}

func (cache *LRUCache) deleteBack() *Node {

	// decrement cache size
	cache.currSize -= 1

	LRUNode := cache.LRUPointer.next

	// determine new LRU node
	newLRUNode := LRUNode.next

	cache.LRUPointer.next = newLRUNode
	newLRUNode.prev = cache.LRUPointer

	LRUNode.next = nil
	LRUNode.prev = nil

	// delete LRU node from map
	delete(cache.PageNodeMap, LRUNode.pageId)

	return LRUNode

}

func (cache *LRUCache) RetrievePage(pageId uint64) ([]byte, bool) {

	// check map to see if node exists.
	node, exists := cache.PageNodeMap[pageId]
	if !exists {
		return nil, exists
	}

	// update last accessed time.
	node.lastAccessed = time.Now()

	// move node to front of the list.
	cache.deleteMiddle(node)
	cache.addFront(node)

	return node.data, true

}

func (cache *LRUCache) CachePage(pageId uint64, data []byte, dirty bool) (evictedPageId *uint64, evictedPageData []byte) {

	// check map to see if node exists.
	node, exists := cache.PageNodeMap[pageId]

	// if page exists
	if exists {

		// update last accessed time.
		node.lastAccessed = time.Now()

		// update dirty flag, data.
		node.dirty = dirty
		node.data = data
		node.pinCount++

		// move node to front of the list.
		cache.deleteMiddle(node)
		cache.addFront(node)

	} else {

		// increment cache size.
		cache.currSize += 1

		// create new node corresponding to page.
		node = newNode(pageId, data, dirty)

		// update map.
		cache.PageNodeMap[pageId] = node

		// add node to front of the list.
		cache.addFront(node)

		// if cache size is greater than max size, then evict LRU node.
		if cache.currSize > cache.maxSize {

			// if evicted node is dirty, return page ID and page data.
			if evictedNode := cache.deleteBack(); evictedNode.dirty {
				return &evictedNode.pageId, evictedNode.data
			}

		}

	}
	return nil, nil
}

func (cache *LRUCache) GetDirtyPages() map[uint64][]byte {

	dirtyPages := map[uint64][]byte{}

	currNode := cache.LRUPointer.next

	for currNode != cache.MRUPointer {

		if currNode.dirty == DIRTY {
			dirtyPages[currNode.pageId] = currNode.data
		}

		currNode = currNode.next
	}

	return dirtyPages
}

func (cache *LRUCache) PinPage(pageId uint64) bool {

	node, exists := cache.PageNodeMap[pageId]

	if !exists {
		return false
	}

	node.pinCount++
	return true

}
func (cache *LRUCache) UnpinPage(pageId uint64) bool {
	node, exists := cache.PageNodeMap[pageId]

	if !exists {
		return false
	}

	node.pinCount--

	return true
}
