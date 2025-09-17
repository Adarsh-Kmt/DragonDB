package main

import (
	"github.com/Adarsh-Kmt/DragonDB/buffer_pool_manager"
	dtl "github.com/Adarsh-Kmt/DragonDB/data_structure_layer"
	"github.com/Adarsh-Kmt/DragonDB/server"
)

func main() {

	cache := buffer_pool_manager.NewLRUReplacer()
	disk, metadata, _, err := buffer_pool_manager.NewDirectIODiskManager("dragon.db")

	if err != nil {
		panic(err)
	}

	bufferPoolManager, err := buffer_pool_manager.NewSimpleBufferPoolManager(5, 4096, cache, disk)

	if err != nil {
		panic(err)
	}

	btree := dtl.NewBTree(0, bufferPoolManager, metadata)

	server, err := server.NewServer(":8080", btree)

	if err != nil {
		panic(err)
	}

	server.Run()
}
