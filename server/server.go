package main

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	"github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	log "github.com/sirupsen/logrus"
)

type Server struct {
	pSocket            *communication.PassiveTCPSocket
	c                  *ServerConfig
	serializer         *data_structures.Serializer
	qMiddleware        *middleware.QueueMiddleware
	outQueueAirports   middleware.ProducerInterface
	outQueueFlightRows middleware.ProducerInterface
}

func NewServer(c *ServerConfig) *Server {
	socket, err := communication.NewPassiveTCPSocket(c.ServerAddress)
	if err != nil {
		log.Fatalf("action: create_server | result: fail | server_id: %v | error: %v", c.ID, err)
	}
	serializer := data_structures.NewSerializer()
	qMiddleware := middleware.NewQueueMiddleware(c.RabbitAddress)
	qA := qMiddleware.CreateExchangeProducer(c.ExchangeNameAirports, c.ExchangeRKAirports, c.ExchangeTypeAirports, true)
	qFR := qMiddleware.CreateProducer(c.QueueNameFlightRows, true)
	return &Server{
		pSocket:            socket,
		c:                  c,
		serializer:         serializer,
		qMiddleware:        qMiddleware,
		outQueueAirports:   qA,
		outQueueFlightRows: qFR,
	}
}

func (svr *Server) handleGetResults(cliSPH *protocol.SocketProtocolHandler) error {
	for i := 0; i < len(svr.c.GetterAddresses); i++ {
		socketGetter, err := communication.NewActiveTCPSocket(svr.c.GetterAddresses[i])
		if err != nil {
			log.Errorf("Error trying to connect to getter for exercise %v. Ending getter conn and returning error.", i+1)
			_ = socketGetter.Close()
			return err
		}
		getterSPH := protocol.NewSocketProtocolHandler(&socketGetter.TCPSocket)
		for {
			msg, err := getterSPH.Read()
			if err != nil {
				log.Errorf("Error trying to read from getter #%v. Ending loop...", i+1)
				_ = socketGetter.Close()
				return err
			}

			// Sends the results to the client.
			// If the getter finishes it notifies the client the end of an exercise results
			err = cliSPH.Write(msg)
			if err != nil {
				log.Errorf("Error trying to send to client. Ending loop...")
				_ = socketGetter.Close()
				return err
			}

			// If the getter finishes we stop the innermost loop
			if msg.TypeMessage == data_structures.EOFGetter {
				_ = socketGetter.Close()
				break
			}
		}
		_ = socketGetter.Close()
	}
	return nil
}

func (svr *Server) handleAirportMessage(message *data_structures.Message) error {
	err := svr.outQueueAirports.Send(svr.serializer.SerializeMsg(message))
	if err != nil {
		return err
	}
	return nil
}

func (svr *Server) handleFlightRowMessage(message *data_structures.Message) error {
	err := svr.outQueueFlightRows.Send(svr.serializer.SerializeMsg(message))
	if err != nil {
		return err
	}
	return nil
}

func (svr *Server) handleMessage(message *data_structures.Message, cliSPH *protocol.SocketProtocolHandler) error {
	if message.TypeMessage == data_structures.Airports || message.TypeMessage == data_structures.EOFAirports {
		return svr.handleAirportMessage(message)
	}
	if message.TypeMessage == data_structures.FlightRows || message.TypeMessage == data_structures.EOFFlightRows {
		return svr.handleFlightRowMessage(message)
	}
	if message.TypeMessage == data_structures.GetResults {
		return svr.handleGetResults(cliSPH)
	}
	return fmt.Errorf("unrecognized message type: %v", message.TypeMessage)
}

func (svr *Server) handleClient(cSock *communication.TCPSocket) {
	sph := protocol.NewSocketProtocolHandler(cSock)
	message, err := sph.Read()
	if err != nil {
		log.Errorf("Error trying to receive message: %v. Ending client handle & Closing socket...", err)
		_ = cSock.Close()
		return
	}
	err = svr.handleMessage(message, sph)
	_ = cSock.Close()
	if err != nil {
		log.Errorf("%v", err)
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

func (svr *Server) End() error {
	svr.qMiddleware.Close()
	return svr.pSocket.Close()
}
