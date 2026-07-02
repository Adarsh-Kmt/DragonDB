package main

import (
	bplustree "github.com/Adarsh-Kmt/DragonDB/bplustree"
	bpm "github.com/Adarsh-Kmt/DragonDB/bufferpoolmanager"
	server "github.com/Adarsh-Kmt/DragonDB/server"
	lucario "github.com/Adarsh-Kmt/Lucario"
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

	wal, err := lucario.NewWAL("./lucario.wal")
	if err != nil {
		panic(err)
	}

	btree := bplustree.NewBPlusTree(0, bufferPoolManager, metadata, wal)

	server, err := server.NewServer(":8080", btree)

	if err != nil {
		panic(err)
	}

	server.Run()
}
