package main

import (
	"client/parsers"
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	log "github.com/sirupsen/logrus"
)

// Client Entity that encapsulates the client
type Client struct {
	conn *protocol.SocketProtocolHandler
	conf *ClientConfig
}

// NewClient Initializes a new client
func NewClient(c *ClientConfig) *Client {
	socket, err := communication.NewActiveTCPSocket(c.ServerAddress)
	if err != nil {
		log.Fatalf("action: connect | result: fail | client_id: %v | error: %v", c.ID, err)
	}
	return &Client{conn: protocol.NewSocketProtocolHandler(socket.TCPSocketInterface), conf: c}
}

// StartClientLoop Sends the flight rows and airport
// In case of error it closes the connection and finishes
func (c *Client) StartClientLoop() {

	defer c.Close()

	err := SendFile(c.conf.AirportFileName, c.conf.Batch, c.conn, parsers.AirportsParser{})
	if err != nil {
		return
	}

	err = SendFile(c.conf.AirportFileName, c.conf.Batch, c.conn, parsers.FlightsParser{})
	if err != nil {
		return
	}
	
	log.Infof("Requesting results")
	msg := &dataStructures.Message{TypeMessage: dataStructures.GetResults}
	err = c.conn.Write(msg)
	if err != nil {
		log.Errorf("Error requesting results: %v", err)
		return
	}
	for {
		msg, err = c.conn.Read()
		if err != nil {
			log.Errorf("Error reading results: %v", err)
			return
		}
		if msg.TypeMessage == dataStructures.EOFFlightRows {
			log.Infof("End results")
			return
		}

		// TODO serializar a string todo para imprimir
	}

}

// Close Closes the client connection to the server
func (c *Client) Close() {
	c.conn.Close()
}
