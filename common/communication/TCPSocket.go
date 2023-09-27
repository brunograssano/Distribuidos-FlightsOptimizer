package communication

import (
	"net"
)

type TCPSocketInterface interface {
	Read(size uint32) ([]byte, error)
	Write(message []byte) (int, error)
	Close() error
}

type TCPSocket struct {
	TCPSocketInterface
	address    string
	connection net.Conn
}

func (tcpSocket *TCPSocket) Read(sizeToRead uint32) ([]byte, error) {
	accum := 0
	buffer := make([]byte, sizeToRead)
	for accum < int(sizeToRead) {
		size, err := tcpSocket.connection.Read(buffer[accum:])
		if err != nil {
			return buffer, err
		}
		accum += size
	}
	return buffer, nil
}

func (tcpSocket *TCPSocket) Write(message []byte) (int, error) {
	accumSent := 0
	for accumSent < len(message) {
		sizeSent, sendErr := tcpSocket.connection.Write(message[accumSent:])
		if sendErr != nil {
			return accumSent, sendErr
		}
		accumSent += sizeSent
	}
	return accumSent, nil
}

func (tcpSocket *TCPSocket) Close() error {
	return tcpSocket.connection.Close()
}
