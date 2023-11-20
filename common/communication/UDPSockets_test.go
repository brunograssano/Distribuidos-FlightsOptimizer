package communication

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCreatingAClientUDPWithoutServerShouldNotThrowError(t *testing.T) {
	udpCli, err := NewUdpClient("127.0.0.1:10000")
	assert.Nilf(t, err, fmt.Sprintf("Should not have thrown error, socket is UDP. Error was: %v.", err))
	udpCli.Close()
}

func TestCreatingAnUDPServerInAnEmptyPortShouldCreateItCorrectly(t *testing.T) {
	udpSvr, err := NewUdpServer("127.0.0.1", 10001)
	assert.Nilf(t, err, fmt.Sprintf("Should not have thrown error. Error was: %v", err))
	assert.NotNilf(t, udpSvr, "UdpServer is nil.")
	udpSvr.Close()
}

func TestCreatingAnUDPServerInAnUsedPortShouldThrowError(t *testing.T) {
	udpSvr, err := NewUdpServer("127.0.0.1", 10002)
	assert.Nilf(t, err, fmt.Sprintf("Should not have thrown error. Error was: %v", err))
	assert.NotNil(t, udpSvr, "UdpServer is nil.")
	udpSvrNil, err := NewUdpServer("127.0.0.1", 10002)
	assert.Errorf(t, err, "Should have thrown error. UDP Port already used...")
	assert.Nilf(t, udpSvrNil, "UDP Server created with port used should be nil.")
	udpSvr.Close()
}

func TestSimpleConnectionAndHelloFromClientToServer(t *testing.T) {
	udpSvr, err := NewUdpServer("127.0.0.1", 10003)
	assert.Nilf(t, err, fmt.Sprintf("Should not have thrown error creating server. Error was: %v", err))
	udpCli, err := NewUdpClient("127.0.0.1:10003")
	assert.Nilf(t, err, fmt.Sprintf("Should not have thrown error creating client. Error was: %v", err))
	msg := []byte("Hello")
	sizeSent, err := udpCli.Send(msg, nil)
	assert.Nilf(t, err, fmt.Sprintf("Should not have thrown error sending to server. Error was :%v", err))
	assert.Equalf(t, len(msg), sizeSent, fmt.Sprintf("Should have sent same size as message. Expected was: %v, sent was: %v", len(msg), sizeSent))
	msgReceived, address, err := udpSvr.Receive(uint(len(msg)))
	assert.Equalf(t, msg, msgReceived, fmt.Sprintf("Expected %v to be equal to %v", msg, msgReceived))
	assert.NotNilf(t, address, "Address received is nil")
	assert.Nilf(t, err, fmt.Sprintf("Error receiving message in udp server: %v", err))
	udpCli.Close()
	udpSvr.Close()
}

func TestSendFromServerToClientAfterReceivingItsAddress(t *testing.T) {
	udpSvr, err := NewUdpServer("127.0.0.1", 10004)
	assert.Nilf(t, err, fmt.Sprintf("Should not have thrown error creating server. Error was: %v", err))
	udpCli, err := NewUdpClient("127.0.0.1:10004")
	assert.Nilf(t, err, fmt.Sprintf("Should not have thrown error creating client. Error was: %v", err))
	msg := []byte("Hello")

	// From client to server
	sizeSent, err := udpCli.Send(msg, nil)
	assert.Nilf(t, err, fmt.Sprintf("Should not have thrown error sending to server. Error was :%v", err))
	assert.Equalf(t, len(msg), sizeSent, fmt.Sprintf("Should have sent same size as message. Expected was: %v, sent was: %v", len(msg), sizeSent))
	msgReceived, address, err := udpSvr.Receive(uint(len(msg)))
	assert.Equalf(t, msg, msgReceived, fmt.Sprintf("Expected %v to be equal to %v", msg, msgReceived))
	assert.NotNilf(t, address, "Address received is nil")
	assert.Nilf(t, err, fmt.Sprintf("Error receiving message in udp server: %v", err))

	// From server to client
	msg = []byte("Hello Server!")
	sizeSent, err = udpSvr.Send(msg, address)
	assert.Nilf(t, err, fmt.Sprintf("Should not have thrown error sending to client. Error was :%v", err))
	assert.Equalf(t, len(msg), sizeSent, fmt.Sprintf("Should have sent same size as message. Expected was: %v, sent was: %v", len(msg), sizeSent))
	msgReceived, address, err = udpCli.Receive(uint(len(msg)))
	assert.Equalf(t, msg, msgReceived, fmt.Sprintf("Expected %v to be equal to %v", msg, msgReceived))
	assert.Nilf(t, address, "Address received is not nil and client is receiving")
	assert.Nilf(t, err, fmt.Sprintf("Error receiving message in udp client: %v", err))

	udpCli.Close()
	udpSvr.Close()
}
