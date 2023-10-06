package communication

import (
	log "github.com/sirupsen/logrus"
	"net"
)

type PassiveTCPSocketInterface interface {
	Accept() (net.Conn, error)
	Close() error
}

type PassiveTCPSocket struct {
	TCPSocket
	PassiveTCPSocketInterface
	listener net.Listener
}

func NewPassiveTCPSocket(address string) (*PassiveTCPSocket, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	return &PassiveTCPSocket{
		TCPSocket: TCPSocket{
			address: address,
		},
		listener: listener,
	}, nil
}

func (passiveTCPSocket *PassiveTCPSocket) Accept() (*TCPSocket, error) {
	conn, err := passiveTCPSocket.listener.Accept()
	log.Infof("Received new connection: %v", conn.LocalAddr())
	if err != nil {
		return nil, err
	}
	return &TCPSocket{
		connection: conn,
		address:    passiveTCPSocket.address,
	}, nil
}

func (passiveTCPSocket *PassiveTCPSocket) Close() error {
	err := passiveTCPSocket.listener.Close()
	return err
}
