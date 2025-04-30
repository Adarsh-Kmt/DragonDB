package main

import (
	"log"
	"log/slog"

	"github.com/Adarsh-Kmt/DragonDB/buffer_pool_manager"
)

func main() {

	cache := buffer_pool_manager.NewLRUReplacer()
	disk, err := buffer_pool_manager.NewDiskManager("/file")

	if err != nil {
		panic(err)
	}

	bufferPool := buffer_pool_manager.NewSimpleBufferPoolManager(5, 4096, cache, disk)

	writeGuard, err := bufferPool.NewWriteGuard(buffer_pool_manager.PageID(1))

	if err != nil {
		slog.Error(err.Error())
	}
	defer writeGuard.Done()

	data, ok := writeGuard.GetData()

	if ok {
		log.Println(data)
	}
}
