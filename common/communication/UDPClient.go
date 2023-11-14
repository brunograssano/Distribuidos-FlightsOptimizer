package communication

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
)

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

func (u *UdpClient) Receive(sizeToRecv uint) ([]byte, *net.UDPAddr, error) {
	buffer := make([]byte, sizeToRecv)
	sizeRead, err := u.conn.Read(buffer)
	if err != nil {
		log.Errorf("UdpClient | Error trying to Read | Size Read: %v | %v", sizeRead, err)
		return nil, nil, err
	}
	if sizeRead != int(sizeToRecv) {
		log.Errorf("UdpClient | Wrong Read | Size Read: %v | Size Expected: %v", sizeRead, sizeToRecv)
		return nil, nil, err
	}
	return buffer, nil, nil
}

func (u *UdpClient) Send(message []byte, _ *net.UDPAddr) (int, error) {
	sizeSent, err := u.conn.Write(message)
	if sizeSent < len(message) {
		log.Errorf("UdpServer | Wrong Write | Size Sent: %v | Size Expected: %v ", sizeSent, len(message))
		return sizeSent, fmt.Errorf("size read differs from size expected")
	}
	if err != nil {
		log.Errorf("UdpServer | Error trying to Write | %v", err)
		return sizeSent, err
	}
	return sizeSent, nil
}

func (u *UdpClient) Close() {
	err := u.conn.Close()
	if err != nil {
		log.Errorf("UdpClient | Error closing socket | %v", err)
	}
}
