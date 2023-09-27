package communication

import "net"

type ActiveTCPSocketInterface interface {
	Reconnect() error
}

type ActiveTCPSocket struct {
	TCPSocket
	ActiveTCPSocketInterface
}

func NewActiveTCPSocket(address string) (*ActiveTCPSocket, error) {
	connection, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return &ActiveTCPSocket{
		TCPSocket: TCPSocket{
			address:    address,
			connection: connection,
		},
	}, nil
}

func (activeTCPSocket *ActiveTCPSocket) Reconnect() error {
	connection, err := net.Dial("tcp", activeTCPSocket.address)
	if err != nil {
		return err
	}
	activeTCPSocket.connection = connection
	return nil
}
