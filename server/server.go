package server

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	dtl "github.com/Adarsh-Kmt/DragonDB/data_structure_layer"
)

type Server struct {
	addr     string
	listener net.Listener

	dataStructureLayer dtl.DataStructureLayer

	shutdown     chan struct{}
	shutdownOnce *sync.Once
}

func NewServer(addr string, dataStructureLayer dtl.DataStructureLayer) (*Server, error) {

	listener, err := net.Listen("tcp", addr)

	if err != nil {
		return nil, err
	}
	return &Server{
		dataStructureLayer: dataStructureLayer,
		listener:           listener,
		addr:               addr,
		shutdown:           make(chan struct{}),
		shutdownOnce:       &sync.Once{},
	}, nil
}

func handleShutdown(conn net.Conn) {

	message := encodeShutdownMessage()

	if _, err := conn.Write(message); err != nil {
		slog.Error(err.Error(), "msg", "error while sending shutdown message")
	}

	if err := conn.Close(); err != nil {
		slog.Error(err.Error(), "msg", "error while closing connection")
	}

}

func (server *Server) handleRequest(conn net.Conn) {

	messageTypeBytes, err := readNBytes(conn, 1)

	// check for read timeout error
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return
	}
	// handle error
	if err != nil {
		slog.Error(err.Error(), "msg", "error while reading message op code")
		response := encodeErrorResponse(err)
		conn.Write(response)
		return
	}

	messageType := string(messageTypeBytes[:])

	switch messageType {

	// handle ping request
	case "P":

		if _, err := conn.Write([]byte("O")); err != nil {
			slog.Error(err.Error(), "msg", "error while sending OK response")
		}

	// handle insert request
	case "I":

		key, value, err := decodeInsertRequestBody(conn)

		// handle error
		if err != nil {
			slog.Error(err.Error(), "msg", "error while decoding insert request")
			response := encodeErrorResponse(err)

			if _, err2 := conn.Write(response); err2 != nil {
				slog.Error(err2.Error(), "msg", "error while writing to connection")
			}
			return
		}

		err = server.dataStructureLayer.Insert(key, value)

		// handle error
		if err != nil {
			slog.Error(err.Error(), "msg", "error occured in data structure layer")
			response := encodeErrorResponse(err)

			if _, err2 := conn.Write(response); err2 != nil {
				slog.Error(err2.Error(), "msg", "error while writing to connection")
			}
		} else {

			response := encodeInsertResponse()

			if _, err = conn.Write(response); err != nil {
				slog.Error(err.Error(), "msg", "error while writing to conn")
			}
		}

	// handle delete request
	case "D":

		key, err := decodeDeleteRequestBody(conn)

		if err != nil {
			slog.Error(err.Error(), "msg", "error while decoding delete request")
			response := encodeErrorResponse(err)
			conn.Write(response)
			return
		}

		err = server.dataStructureLayer.Delete(key)

		// handle error
		if err != nil {
			slog.Error(err.Error(), "msg", "error occured in data structure layer")
			response := encodeErrorResponse(err)

			if _, err2 := conn.Write(response); err2 != nil {
				slog.Error(err2.Error(), "msg", "error while writing to connection")
			}

		} else {
			response := encodeDeleteResponse()
			if _, err = conn.Write(response); err != nil {
				slog.Error(err.Error(), "msg", "error while writing to conn")
			}
		}

	// handle get request
	case "G":

		key, err := decodeGetRequestBody(conn)

		if err != nil {
			slog.Error(err.Error(), "msg", "error while decoding get request")
			response := encodeErrorResponse(err)

			if _, err2 := conn.Write(response); err2 != nil {
				slog.Error(err2.Error(), "msg", "error while writing to connection")
			}
			return
		}
		slog.Info(fmt.Sprintf("received get request for key %d", key))
		value, err := server.dataStructureLayer.Get(key)

		// handle error
		if err != nil {
			slog.Error(err.Error(), "msg", "error occured in data structure layer")
			response := encodeErrorResponse(err)

			if _, err2 := conn.Write(response); err2 != nil {
				slog.Error(err2.Error(), "msg", "error while writing to connection")
			}

		} else {
			response := encodeGetResponse(key, value)

			if _, err = conn.Write(response); err != nil {
				slog.Error(err.Error(), "msg", "error while writing to conn")
			}
		}

	// handle close request
	case "C":

		if err := conn.Close(); err != nil {
			slog.Error(err.Error(), "msg", "error while closing connection")
		}

	// handle shutdown request
	case "S":
		server.Shutdown()

	default:
		slog.Error("invalid op code")
		response := encodeErrorResponse(fmt.Errorf("invalid op code"))

		if _, err := conn.Write(response); err != nil {
			slog.Error(err.Error(), "msg", "error while writing to connection")
		}

	}

}
func (server *Server) handleClient(conn net.Conn, wg *sync.WaitGroup) {

	defer wg.Done()
	conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	for {

		select {

		case <-server.shutdown:
			slog.Info("client exiting...")
			handleShutdown(conn)
			return

		default:

			server.handleRequest(conn)
		}

	}

}

func (server *Server) listen(listenerWaitGroup, clientWaitGroup *sync.WaitGroup) {

	defer listenerWaitGroup.Done()

	for {

		conn, err := server.listener.Accept()
		if errors.Is(err, net.ErrClosed) {
			slog.Error(err.Error(), "msg", "listener closed")
			return
		}
		slog.Info("client joined from " + conn.LocalAddr().String())
		clientWaitGroup.Add(1)
		go server.handleClient(conn, clientWaitGroup)

	}

}

func (server *Server) Run() {

	clientWaitGroup := &sync.WaitGroup{}
	listenerWaitGroup := &sync.WaitGroup{}

	listenerWaitGroup.Add(1)
	go server.listen(listenerWaitGroup, clientWaitGroup)

	slog.Info("waiting for shutdown...")
	listenerWaitGroup.Wait()
	slog.Info("waiting for clients to exit...")
	clientWaitGroup.Wait()
}

func (server *Server) Shutdown() {

	slog.Info("shutdown initiated...")
	server.shutdownOnce.Do(func() {
		server.listener.Close()
		close(server.shutdown)

	})

}
