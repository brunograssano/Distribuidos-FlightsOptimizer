package communication

import (
	log "github.com/sirupsen/logrus"
	"net"
)

type UdpServer struct {
	listener *net.UDPConn
	address  net.UDPAddr
}

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

func (u *UdpServer) Listen() {
	ser, err := u.listener.ReadFromUDP()
}

type UdpClient struct {
	conn net.Conn
}

func NewUdpClient(address string) (*UdpClient, error) {
	conn, err := net.Dial("udp", address)
	if err != nil {
		log.Errorf("UdpClient | Error trying to create | %v", err)
		return nil, err
	}
	return &UdpClient{
			conn: conn,
		},
		nil
}

func (u *UdpClient) Send() {

}
