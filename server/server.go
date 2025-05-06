package server

import (
	"net"

	dtl "github.com/Adarsh-Kmt/DragonDB/data_structure_layer"
)

type Server struct {
	addr               string
	dataStructureLayer dtl.DataStructureLayer
}

func NewServer(addr string, dataStructureLayer dtl.DataStructureLayer) *Server {

	return &Server{
		dataStructureLayer: dataStructureLayer,
		addr:               addr,
	}
}
func (server *Server) HandleClient(conn net.Conn) error {

	for {

		messageTypeBytes, err := readNBytes(conn, 1)

		// handle error
		if err != nil {
			response := encodeErrorResponse(err)

			conn.Write(response)
			continue
		}

		messageType := string(messageTypeBytes[:])

		// handle ping
		if messageType == "P" {

			conn.Write([]byte("O"))
		}

		// handle insert
		if messageType == "I" {

			key, value, err := decodeInsertRequest(conn)

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

		}

		// handle delete
		if messageType == "D" {

			key, err := decodeDeleteRequest(conn)

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
		}

		// handle get
		if messageType == "G" {

			key, err := decodeGetRequest(conn)

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
func main() {

	server := NewServer(":addr", dtl.NewHashMap())
	server.Run()
}
