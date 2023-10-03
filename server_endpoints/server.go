package server_endpoints

import (
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	"github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	log "github.com/sirupsen/logrus"
)

type Server struct {
	pSocket    *communication.PassiveTCPSocket
	c          *ServerConfig
	serializer *data_structures.Serializer
}

func NewServer(c *ServerConfig) *Server {
	socket, err := communication.NewPassiveTCPSocket(c.ServerAddress)
	if err != nil {
		log.Fatalf("action: create_server | result: fail | server_id: %v | error: %v", c.ID, err)
	}
	serializer := data_structures.NewSerializer()
	return &Server{
		pSocket:    socket,
		c:          c,
		serializer: serializer,
	}
}

/*
func (svr *Server) handleGetResults(cliSocket) {
	for i := 0; i < len(svr.c.GetterAddresses); i++ {
		socket, err := communication.NewPassiveTCPSocket(svr.c.GetterAddresses[i])
		if err != nil {
			log.Errorf("Error trying to connect to getter for exercise %v", i+1)
			continue
		}
		socket.Read()
	}
}

func (svr *Server) handleMessage(message *data_structures.DynamicMap, cliSocket *communication.TCPSocket) error {
	typeMsg, err := message.GetAsString("type")
	if err != nil {
		return err
	}
	switch typeMsg {
	case "Airport":
		svr.handleAirportMessage(message)
	case "FlightRow":
		svr.handleFlightRowMessage(message)
	case "GetResults":
		svr.handleGetResults(cliSocket)
	}
}

func (svr *Server) handleClient(cliSocket *communication.TCPSocket) {
	length, err := svr.getLengthOfMessage(cliSocket)
	if err != nil {
		log.Errorf("%v", err)
		_ = cliSocket.Close()
		return
	}
	message, err := svr.receiveMessage(cliSocket, length)
	if err != nil {
		log.Errorf("%v", err)
		_ = cliSocket.Close()
		return
	}
	err = svr.handleMessage(message, cliSocket)
	if err != nil {
		log.Errorf("%v", err)
		_ = cliSocket.Close()
		return
	}
}

func (svr *Server) StartServerLoop() {
	for {
		accepted, err := svr.pSocket.Accept()
		if err != nil {
			log.Errorf("Error trying to accept connection: %v. Finishing loop...", err)
			break
		}
		go svr.handleClient(accepted)
	}
}
*/
