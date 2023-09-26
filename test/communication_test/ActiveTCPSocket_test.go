package communication_test

import (
	"DistribuidosTP1/communication"
	"bytes"
	"net"
	"testing"
)

func AcceptClient(listener net.Listener, returnChannel chan net.Conn) {
	accept, err := listener.Accept()
	if err != nil {
		returnChannel <- nil
	}
	returnChannel <- accept
}

func TestCreatingAnActiveSocketWithoutServerThrowsError(t *testing.T) {
	_, err := communication.NewActiveTCPSocket("127.0.0.1:9000")
	if err == nil {
		t.Errorf("Should have thrown error, server does not exist.")
	}
}

func TestCreatingAnActiveSocketWithServerCreatesItOk(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:9001")
	retChan := make(chan net.Conn)
	go AcceptClient(listener, retChan)
	if err != nil {
		t.Errorf("Error creating the server: %v", err)
	}
	activeTcpSocket, errCli := communication.NewActiveTCPSocket("127.0.0.1:9001")
	if errCli != nil {
		t.Errorf("Thrown Error on creating ActiveSocket: %v", errCli)
	}
	serverConn := <-retChan
	if serverConn == nil {
		t.Errorf("Error on receiveing conn in server.")
	}
	errSvrClose := serverConn.Close()
	if errSvrClose != nil {
		t.Errorf("Error on closing server conn")
	}
	errCliClose := activeTcpSocket.Close()
	if errCliClose != nil {
		t.Errorf("Error on closing client conn")
	}
	errListClose := listener.Close()
	if errListClose != nil {
		t.Errorf("Error closing listener")
	}
	close(retChan)
}

func TestSendShortMessageInActiveSocket(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:9002")
	retChan := make(chan net.Conn)
	go AcceptClient(listener, retChan)
	if err != nil {
		t.Errorf("Error creating the server: %v", err)
	}
	activeTcpSocket, errCli := communication.NewActiveTCPSocket("127.0.0.1:9002")
	if errCli != nil {
		t.Errorf("Thrown Error on creating ActiveSocket: %v", errCli)
	}
	serverConn := <-retChan
	if serverConn == nil {
		t.Errorf("Error on receiveing conn in server.")
	}
	msg := []byte("Test Message")
	write, err := activeTcpSocket.Write(msg)
	if write != len(msg) {
		t.Errorf("Error writing in Active Socket. Written was: %v and expected: %v", write, len(msg))
	}
	if err != nil {
		t.Errorf("Error writing in Active Socket: %v", err)
	}
	buffer := make([]byte, len(msg))
	read, err := serverConn.Read(buffer)
	if read < len(msg) || err != nil {
		t.Errorf("Wrong read in server's client socket")
	}
	if !bytes.Equal(msg, buffer) {
		t.Errorf("Message was: %v and received in buffer is: %v", msg, buffer)
	}

	_ = serverConn.Close()
	_ = activeTcpSocket.Close()
	_ = listener.Close()
	close(retChan)
}

func TestSendLongMessageInActiveSocket(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:9003")
	retChan := make(chan net.Conn)
	go AcceptClient(listener, retChan)
	if err != nil {
		t.Errorf("Error creating the server: %v", err)
	}
	activeTcpSocket, errCli := communication.NewActiveTCPSocket("127.0.0.1:9003")
	if errCli != nil {
		t.Errorf("Thrown Error on creating ActiveSocket: %v", errCli)
	}
	serverConn := <-retChan
	if serverConn == nil {
		t.Errorf("Error on receiveing conn in server.")
	}
	str := "a"
	for len(str) < 100000 {
		str += "a"
	}
	msg := []byte(str)
	write, err := activeTcpSocket.Write(msg)
	if write != len(msg) {
		t.Errorf("Error writing in Active Socket. Written was: %v and expected: %v", write, len(msg))
	}
	if err != nil {
		t.Errorf("Error writing in Active Socket: %v", err)
	}
	accum := 0
	buffer := make([]byte, len(msg))
	for accum < len(msg) {
		read, err := serverConn.Read(buffer[accum:])
		accum += read
		if err != nil {
			t.Errorf("Wrong read in server's client socket")
		}
	}
	if accum < len(msg) {
		t.Errorf("Wrong size read in server's client socket")
	}
	if !bytes.Equal(msg, buffer) {
		t.Errorf("Message was: %v and received in buffer is: %v", msg, buffer)
	}

	_ = serverConn.Close()
	_ = activeTcpSocket.Close()
	_ = listener.Close()
	close(retChan)
}

func TestLongReadMessageInActiveSocket(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:9004")
	retChan := make(chan net.Conn)
	go AcceptClient(listener, retChan)
	if err != nil {
		t.Errorf("Error creating the server: %v", err)
	}
	activeTcpSocket, errCli := communication.NewActiveTCPSocket("127.0.0.1:9004")
	if errCli != nil {
		t.Errorf("Thrown Error on creating ActiveSocket: %v", errCli)
	}
	serverConn := <-retChan
	if serverConn == nil {
		t.Errorf("Error on receiveing conn in server.")
	}
	str := "a"
	for len(str) < 100000 {
		str += "a"
	}
	accum := 0
	msg := []byte(str)
	for accum < len(msg) {
		wrtn, err := serverConn.Write(msg[accum:])
		accum += wrtn
		if err != nil {
			t.Errorf("Wrong read in server's client socket")
		}
	}
	if accum != len(msg) {
		t.Errorf("Error writing in Server Client Socket. Written was: %v and expected: %v", accum, len(msg))
	}
	if err != nil {
		t.Errorf("Error writing in Server Client Socket: %v", err)
	}
	read, err := activeTcpSocket.Read(uint32(len(msg)))
	if len(read) < len(msg) || err != nil {
		t.Errorf("Wrong read in ActiveTCPSocket")
	}
	if !bytes.Equal(msg, read) {
		t.Errorf("Message was: %v and received in buffer is: %v", msg, read)
	}

	_ = serverConn.Close()
	_ = activeTcpSocket.Close()
	_ = listener.Close()
	close(retChan)
}

func TestShortReadMessageInActiveSocket(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:9005")
	retChan := make(chan net.Conn)
	go AcceptClient(listener, retChan)
	if err != nil {
		t.Errorf("Error creating the server: %v", err)
	}
	activeTcpSocket, errCli := communication.NewActiveTCPSocket("127.0.0.1:9005")
	if errCli != nil {
		t.Errorf("Thrown Error on creating ActiveSocket: %v", errCli)
	}
	serverConn := <-retChan
	if serverConn == nil {
		t.Errorf("Error on receiveing conn in server.")
	}
	msg := []byte("Test Message")
	write, err := serverConn.Write(msg)
	if write != len(msg) {
		t.Errorf("Error writing in Server Client Socket. Written was: %v and expected: %v", write, len(msg))
	}
	if err != nil {
		t.Errorf("Error writing in Server Client Socket: %v", err)
	}
	read, err := activeTcpSocket.Read(uint32(len(msg)))
	if len(read) < len(msg) || err != nil {
		t.Errorf("Wrong read in ActiveTCPSocket")
	}
	if !bytes.Equal(msg, read) {
		t.Errorf("Message was: %v and received in buffer is: %v", msg, read)
	}

	_ = serverConn.Close()
	_ = activeTcpSocket.Close()
	_ = listener.Close()
	close(retChan)
}
