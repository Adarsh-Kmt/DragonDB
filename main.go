package main

import "DragonDB/buffer_pool_manager"

func main() {

	cache := buffer_pool_manager.NewLRUCache(5)
	disk, err := buffer_pool_manager.NewDiskManager("/file")

	if err != nil {
		panic(err)
	}

	bufferPool := buffer_pool_manager.NewSimpleBufferPoolManager(cache, disk)
}
