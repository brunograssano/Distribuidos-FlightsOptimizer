package communication

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
)

func AcceptClientTest(listener net.Listener, returnChannel chan net.Conn) {
	accept, err := listener.Accept()
	if err != nil {
		returnChannel <- nil
	}
	returnChannel <- accept
}

func TestCreatingAnActiveSocketWithoutServerThrowsError(t *testing.T) {
	_, err := NewActiveTCPSocket("127.0.0.1:9000")
	assert.Error(t, err, "Should have thrown error, server does not exist.")
}

func TestCreatingAnActiveSocketWithServerCreatesItOk(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:9001")
	retChan := make(chan net.Conn)
	go AcceptClientTest(listener, retChan)
	assert.Nilf(t, err, "Error creating the server: %v", err)

	activeTcpSocket, errCli := NewActiveTCPSocket("127.0.0.1:9001")
	assert.Nilf(t, errCli, "Thrown Error on creating ActiveSocket: %v", errCli)

	serverConn := <-retChan
	assert.NotNil(t, serverConn, "Error on receiveing conn in server.")

	errSvrClose := serverConn.Close()
	assert.Nilf(t, errSvrClose, "Error on closing server conn")

	errCliClose := activeTcpSocket.Close()
	assert.Nilf(t, errCliClose, "Error on closing client conn")

	errListClose := listener.Close()
	assert.Nilf(t, errListClose, "Error closing listener")
	close(retChan)
}

func TestSendShortMessageInActiveSocket(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:9002")
	retChan := make(chan net.Conn)
	go AcceptClientTest(listener, retChan)
	assert.Nilf(t, err, "Error creating the server: %v", err)

	activeTcpSocket, errCli := NewActiveTCPSocket("127.0.0.1:9002")
	assert.Nilf(t, errCli, "Thrown Error on creating ActiveSocket: %v", errCli)

	serverConn := <-retChan
	assert.NotNil(t, serverConn, "Error on receiveing conn in server.")

	msg := []byte("Test Message")
	write, err := activeTcpSocket.Write(msg)
	assert.Equalf(t, len(msg), write, "Error writing in Active Socket. Written was: %v and expected: %v", write, len(msg))
	assert.Nilf(t, err, "Error writing in Active Socket: %v", err)

	buffer := make([]byte, len(msg))
	read, err := serverConn.Read(buffer)
	assert.True(t, read == len(msg) && err == nil, "Wrong read in server's client socket")
	assert.True(t, bytes.Equal(msg, buffer), "Message was: %v and received in buffer is: %v", msg, buffer)

	_ = serverConn.Close()
	_ = activeTcpSocket.Close()
	_ = listener.Close()
	close(retChan)
}

func TestSendLongMessageInActiveSocket(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:9003")
	retChan := make(chan net.Conn)
	go AcceptClientTest(listener, retChan)
	assert.Nilf(t, err, "Error creating the server: %v", err)

	activeTcpSocket, errCli := NewActiveTCPSocket("127.0.0.1:9003")
	assert.Nilf(t, errCli, "Thrown Error on creating ActiveSocket: %v", errCli)

	serverConn := <-retChan
	assert.NotNil(t, serverConn, "Error on receiveing conn in server.")

	str := "a"
	for len(str) < 100000 {
		str += "a"
	}
	msg := []byte(str)
	write, err := activeTcpSocket.Write(msg)
	assert.Equalf(t, len(msg), write, "Error writing in Active Socket. Written was: %v and expected: %v", write, len(msg))
	assert.Nilf(t, err, "Error writing in Active Socket: %v", err)

	accum := 0
	buffer := make([]byte, len(msg))
	for accum < len(msg) {
		read, err := serverConn.Read(buffer[accum:])
		accum += read
		assert.Nilf(t, err, "Wrong read in server's client socket")
	}
	assert.Equalf(t, len(msg), accum, "Wrong size read in server's client socket")
	assert.True(t, bytes.Equal(msg, buffer), "Message was: %v and received in buffer is: %v", msg, buffer)

	_ = serverConn.Close()
	_ = activeTcpSocket.Close()
	_ = listener.Close()
	close(retChan)
}

func TestLongReadMessageInActiveSocket(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:9004")
	retChan := make(chan net.Conn)
	go AcceptClientTest(listener, retChan)
	assert.Nilf(t, err, "Error creating the server: %v", err)

	activeTcpSocket, errCli := NewActiveTCPSocket("127.0.0.1:9004")
	assert.Nilf(t, errCli, "Thrown Error on creating ActiveSocket: %v", errCli)

	serverConn := <-retChan
	assert.NotNil(t, serverConn, "Error on receiveing conn in server.")

	str := "a"
	for len(str) < 100000 {
		str += "a"
	}
	accum := 0
	msg := []byte(str)
	for accum < len(msg) {
		wrtn, err := serverConn.Write(msg[accum:])
		accum += wrtn
		assert.Nilf(t, err, "Wrong read in server's client socket")
	}
	assert.Equalf(t, len(msg), accum, "Error writing in Server Client Socket. Written was: %v and expected: %v", accum, len(msg))
	assert.Nilf(t, err, "Error writing in Server Client Socket: %v", err)

	read, err := activeTcpSocket.Read(uint32(len(msg)))
	assert.True(t, len(read) == len(msg) && err == nil, "Wrong read in ActiveTCPSocket")
	assert.True(t, bytes.Equal(msg, read), "Message was: %v and received in buffer is: %v", msg, read)

	_ = serverConn.Close()
	_ = activeTcpSocket.Close()
	_ = listener.Close()
	close(retChan)
}

func TestShortReadMessageInActiveSocket(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:9005")
	retChan := make(chan net.Conn)
	go AcceptClientTest(listener, retChan)
	assert.Nilf(t, err, "Error creating the server: %v", err)

	activeTcpSocket, errCli := NewActiveTCPSocket("127.0.0.1:9005")
	assert.Nilf(t, errCli, "Thrown Error on creating ActiveSocket: %v", errCli)

	serverConn := <-retChan
	assert.NotNil(t, serverConn, "Error on receiveing conn in server.")

	msg := []byte("Test Message")
	write, err := serverConn.Write(msg)
	assert.Equalf(t, len(msg), write, "Error writing in Server Client Socket. Written was: %v and expected: %v", write, len(msg))
	assert.Nilf(t, err, "Error writing in Server Client Socket: %v", err)

	read, err := activeTcpSocket.Read(uint32(len(msg)))
	assert.True(t, len(read) == len(msg) && err == nil, "Wrong read in ActiveTCPSocket")
	assert.True(t, bytes.Equal(msg, read), "Message was: %v and received in buffer is: %v", msg, read)

	_ = serverConn.Close()
	_ = activeTcpSocket.Close()
	_ = listener.Close()
	close(retChan)
}
