package communication

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"time"
)

type UdpServer struct {
	listener *net.UDPConn
	address  net.UDPAddr
}

const UdpServerTimeout = 20

func NewUdpServer(address string, port int) (*UdpServer, error) {
	addr := net.UDPAddr{
		Port: port,
		IP:   net.ParseIP(address),
	}
	listener, err := net.ListenUDP("udp", &addr)
	if err != nil {
		return nil, err
	}
	log.Infof("UDP Socket | Created new server at: %v", address)
	return &UdpServer{
		listener: listener,
		address:  addr,
	}, nil
}

func (u *UdpServer) Receive(sizeToRecv uint) ([]byte, *net.UDPAddr, error) {
	err := u.listener.SetReadDeadline(time.Now().Add(UdpServerTimeout * time.Second))
	if err != nil {
		log.Errorf("UdpClient | Error setting read timeout | %v", err)
	}
	buffer := make([]byte, sizeToRecv)
	sizeRead, address, err := u.listener.ReadFromUDP(buffer)
	if err != nil {
		if netErr, ok := err.(net.Error); !(ok && netErr.Timeout()) {
			log.Errorf("UdpServer | Error trying to Read | Address: %v | Size Read: %v | %v", address, sizeRead, err)
		}
		return nil, address, err
	}
	if sizeRead != int(sizeToRecv) {
		log.Errorf("UdpServer | Wrong Read | Address: %v | Size Read: %v | Size Expected: %v", address, sizeRead, sizeToRecv)
		return nil, address, err
	}
	return buffer, address, nil
}

func (u *UdpServer) Send(message []byte, address *net.UDPAddr) (int, error) {
	sizeSent, err := u.listener.WriteToUDP(message, address)
	if err != nil {
		log.Errorf("UdpServer | Error trying to Write | Address: %v | %v", address, err)
		return sizeSent, err
	}
	if sizeSent < len(message) {
		log.Errorf("UdpServer | Wrong Write | Address: %v | Size Sent: %v | Size Expected: %v ", address, sizeSent, len(message))
		return sizeSent, fmt.Errorf("size read differs from size expected")
	}
	return sizeSent, nil
}

func (u *UdpServer) Close() {
	err := u.listener.Close()
	if err != nil {
		log.Errorf("UdpServer | Error closing listener | %v", err)
	}
}
