package main

import (
	dtl "github.com/Adarsh-Kmt/DragonDB/data_structure_layer"
	"github.com/Adarsh-Kmt/DragonDB/server"
)

func main() {

	// cache := buffer_pool_manager.NewLRUReplacer()
	// disk, err := buffer_pool_manager.NewDirectIODiskManager("/file")

	// if err != nil {
	// 	panic(err)
	// }

	// bufferPool := buffer_pool_manager.NewSimpleBufferPoolManager(5, 4096, cache, disk)

	// writeGuard, err := bufferPool.NewWriteGuard(buffer_pool_manager.PageID(1))

	// if err != nil {
	// 	slog.Error(err.Error())
	// }
	// defer writeGuard.Done()

	// data, ok := writeGuard.GetData()

	// if ok {
	// 	log.Println(data)
	// }

	server, err := server.NewServer(":8080", dtl.NewHashMap())

	if err != nil {
		panic(err)
	}
	server.Run()
}
