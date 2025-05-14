package server

import "github.com/stretchr/testify/suite"

type DatabaseServerTestSuite struct {
	suite.Suite
	server *Server
}
