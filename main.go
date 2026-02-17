package main

import (
	bplustree "github.com/Adarsh-Kmt/DragonDB/bplustree"
	bpm "github.com/Adarsh-Kmt/DragonDB/bufferpoolmanager"
	"github.com/Adarsh-Kmt/DragonDB/server"
)

func main() {

	cache := bpm.NewLRUReplacer()
	disk, metadata, _, err := bpm.NewDirectIODiskManager("dragon.db")

	if err != nil {
		panic(err)
	}

	bufferPoolManager, err := bpm.NewSimpleBufferPoolManager(5, 4096, cache, disk)

	if err != nil {
		panic(err)
	}

	btree := bplustree.NewBPlusTree(0, bufferPoolManager, metadata)

	server, err := server.NewServer(":8080", btree)

	if err != nil {
		panic(err)
	}

	server.Run()
}
