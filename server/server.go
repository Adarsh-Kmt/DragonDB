package server

import (
	"net"
	"sync"

	dtl "github.com/Adarsh-Kmt/DragonDB/data_structure_layer"
)

type Server struct {
	addr               string
	dataStructureLayer dtl.DataStructureLayer
	connections        []*DatabaseConnection
}

type DatabaseConnection struct {
	conn            net.Conn
	shutdown        bool
	mutex           *sync.Mutex
	pendingRequests int
}

func NewServer(addr string, dataStructureLayer dtl.DataStructureLayer) *Server {

	return &Server{
		dataStructureLayer: dataStructureLayer,
		addr:               addr,
		connections:        make([]*DatabaseConnection, 0),
	}
}
func (server *Server) HandleClient(conn net.Conn) {

	for {

		messageTypeBytes, err := readNBytes(conn, 1)

		// handle error
		if err != nil {
			response := encodeErrorResponse(err)
			conn.Write(response)
			continue
		}

		messageType := string(messageTypeBytes[:])

		switch messageType {

		// handle ping request
		case "P":

			conn.Write([]byte("O"))

		// handle insert request
		case "I":

			key, value, err := decodeInsertRequestBody(conn)

			// handle error
			if err != nil {
				response := encodeErrorResponse(err)
				conn.Write(response)
				continue
			}

			err = server.dataStructureLayer.Insert(key, value)

			// handle error
			if err != nil {
				response := encodeErrorResponse(err)
				conn.Write(response)
			} else {
				response := encodeInsertResponse()
				conn.Write(response)
			}

		// handle delete request
		case "D":

			key, err := decodeDeleteRequestBody(conn)

			if err != nil {
				response := encodeErrorResponse(err)
				conn.Write(response)
				continue
			}

			err = server.dataStructureLayer.Delete(key)

			// handle error
			if err != nil {
				response := encodeErrorResponse(err)
				conn.Write(response)

			} else {
				response := encodeDeleteResponse()
				conn.Write(response)
			}

		// handle get request
		case "G":

			key, err := decodeGetRequestBody(conn)

			if err != nil {
				response := encodeErrorResponse(err)
				conn.Write(response)
				continue
			}

			value, err := server.dataStructureLayer.Get(key)

			// handle error
			if err != nil {
				response := encodeErrorResponse(err)
				conn.Write(response)

			} else {
				response := encodeGetResponse(key, value)
				conn.Write(response)
			}

		// handle close request
		case "C":

			conn.Close()
			return

		}

	}

}

func (server *Server) Run() error {

	listener, err := net.Listen("tcp", server.addr)

	if err != nil {
		return err
	}

	for {

		conn, err := listener.Accept()

		if err != nil {
			return err
		}

		go server.HandleClient(conn)
	}
}

func (dbconn *DatabaseConnection) 

func (server *Server) Shutdown() error {

}
