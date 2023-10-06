package main

import (
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

// Client Entity that encapsulates the client
type Client struct {
	conn communication.TCPSocketInterface
	c    *ClientConfig
}

// NewClient Initializes a new client
func NewClient(c *ClientConfig) *Client {
	socket, err := communication.NewActiveTCPSocket(c.ServerAddress)
	if err != nil {
		log.Fatalf("action: connect | result: fail | client_id: %v | error: %v", c.ID, err)
	}
	return &Client{conn: socket, c: c}
}

// StartClientLoop Sends the flight rows and airport
// In case of error it closes the connection and finishes
func (c *Client) StartClientLoop() {

	// Send airports
	// Send flight rows
	// Request results

}

func (c *Client) Close() {
	utils.CloseSocketAndNotifyError(c.conn)
}
