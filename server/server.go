package main

import (
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	log "github.com/sirupsen/logrus"
)

type Server struct {
	pSocket            *communication.PassiveTCPSocket
	c                  *ServerConfig
	qMiddleware        *middleware.QueueMiddleware
	outQueueAirports   middleware.ProducerInterface
	outQueueFlightRows middleware.ProducerInterface
}

func NewServer(c *ServerConfig) *Server {
	socket, err := communication.NewPassiveTCPSocket(c.ServerAddress)
	if err != nil {
		log.Fatalf("Server | action: create_server | result: fail | server_id: %v | error: %v", c.ID, err)
	}
	qMiddleware := middleware.NewQueueMiddleware(c.RabbitAddress)
	qA := qMiddleware.CreateExchangeProducer(c.ExchangeNameAirports, c.ExchangeRKAirports, c.ExchangeTypeAirports, true)
	qFR := qMiddleware.CreateProducer(c.QueueNameFlightRows, true)
	return &Server{
		pSocket:            socket,
		c:                  c,
		qMiddleware:        qMiddleware,
		outQueueAirports:   qA,
		outQueueFlightRows: qFR,
	}
}

func (svr *Server) StartServerLoop() {
	for {
		accepted, err := svr.pSocket.Accept()
		if err != nil {
			log.Errorf("Server | Error trying to accept connection | %v | Finishing loop...", err)
			break
		}
		ch := NewClientHandler(
			accepted,
			svr.outQueueAirports,
			svr.outQueueFlightRows,
			svr.c.GetterAddresses,
		)
		ch.StartClientLoop()
	}
}

func (svr *Server) End() error {
	svr.qMiddleware.Close()
	return svr.pSocket.Close()
}
