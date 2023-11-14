package communication

import "net"

type UDPSocket interface {
	Receive(sizeToRecv uint) ([]byte, *net.UDPAddr, error)
	Send(message []byte, address *net.UDPAddr) (int, error)
	Close()
}
