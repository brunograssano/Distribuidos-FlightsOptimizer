package communication

import (
	"bytes"
	"net"
	"testing"
)

func AcceptClient(passiveSocket *PassiveTCPSocket, returnChannel chan *TCPSocket) {
	accept, err := passiveSocket.Accept()
	if err != nil {
		returnChannel <- nil
	}
	returnChannel <- accept
}

func TestPassiveSocketCanBeCreatedWhenPortIsFree(t *testing.T) {
	passiveSocket, err := NewPassiveTCPSocket("127.0.0.1:10000")
	if err != nil {
		t.Errorf("Thrown error on creation: %v", err)
	}
	err = passiveSocket.Close()
	if err != nil {
		t.Errorf("Thrown error on close: %v", err)
	}
}

func TestPassiveSocketThrowsErrorOnCreationWhenPortIsNotFree(t *testing.T) {
	passiveSocket, err := NewPassiveTCPSocket("127.0.0.1:10001")
	if err != nil {
		t.Errorf("Thrown error on creation: %v", err)
	}
	_, errPort := NewPassiveTCPSocket("127.0.0.1:10001")
	if errPort == nil {
		t.Errorf("Should have thrown error on creation when port is used.")
	}
	err = passiveSocket.Close()
	if err != nil {
		t.Errorf("Thrown error on close: %v", err)
	}
}

func TestPassiveSocketShortWrite(t *testing.T) {
	passiveSocket, err := NewPassiveTCPSocket("127.0.0.1:10002")
	if err != nil {
		t.Errorf("Thrown error on creation: %v", err)
	}
	returnChan := make(chan *TCPSocket)
	defer close(returnChan)
	go AcceptClient(passiveSocket, returnChan)
	conn, errConn := net.Dial("tcp", "127.0.0.1:10002")
	if errConn != nil {
		t.Errorf("Thrown error on conn creation: %v", errConn)
	}
	clientSocket := <-returnChan
	msg := []byte("Message")
	lenWr, errWr := clientSocket.Write(msg)
	if errWr != nil {
		t.Errorf("Thrown error on write: %v", errWr)
	}
	if lenWr != len(msg) {
		t.Errorf("Wrong length of write")
	}
	buf := make([]byte, len(msg))
	sizeRead, errRead := conn.Read(buf)
	if sizeRead != len(msg) || bytes.Compare(buf, msg) != 0 {
		t.Errorf("Wrong read.")
	}
	if errRead != nil {
		t.Errorf("Error reading message in client socket")
	}

	_ = conn.Close()
	_ = clientSocket.Close()
	_ = passiveSocket.Close()
}

func TestPassiveSocketLongWrite(t *testing.T) {
	passiveSocket, err := NewPassiveTCPSocket("127.0.0.1:10003")
	if err != nil {
		t.Errorf("Thrown error on creation: %v", err)
	}
	returnChan := make(chan *TCPSocket)
	defer close(returnChan)
	go AcceptClient(passiveSocket, returnChan)
	conn, errConn := net.Dial("tcp", "127.0.0.1:10003")
	if errConn != nil {
		t.Errorf("Thrown error on conn creation: %v", errConn)
	}
	clientSocket := <-returnChan
	str := "a"
	for len(str) < 100000 {
		str += "a"
	}
	msg := []byte(str)
	lenWr, errWr := clientSocket.Write(msg)
	if errWr != nil {
		t.Errorf("Thrown error on write: %v", errWr)
	}
	if lenWr != len(msg) {
		t.Errorf("Wrong length of write")
	}
	buf := make([]byte, len(msg))
	accum := 0
	for accum < len(msg) {
		sizeRead, errRead := conn.Read(buf[accum:])
		accum += sizeRead
		if errRead != nil {
			t.Errorf("Error reading message in client socket")
		}
	}
	if accum != len(msg) || bytes.Compare(buf, msg) != 0 {
		t.Errorf("Wrong read.")
	}

	_ = conn.Close()
	_ = clientSocket.Close()
	_ = passiveSocket.Close()
}

func TestPassiveSocketShortRead(t *testing.T) {
	passiveSocket, err := NewPassiveTCPSocket("127.0.0.1:10004")
	if err != nil {
		t.Errorf("Thrown error on creation: %v", err)
	}
	returnChan := make(chan *TCPSocket)
	defer close(returnChan)
	go AcceptClient(passiveSocket, returnChan)
	conn, errConn := net.Dial("tcp", "127.0.0.1:10004")
	if errConn != nil {
		t.Errorf("Thrown error on conn creation: %v", errConn)
	}
	clientSocket := <-returnChan
	msg := []byte("Message")
	sizeWr, errWr := conn.Write(msg)
	if sizeWr < len(msg) {
		t.Errorf("Error on writing in client socket. Wrong size.")
	}
	if errWr != nil {
		t.Errorf("Error when writing in client socket: %v", errWr)
	}
	bufRead, errRead := clientSocket.Read(uint32(len(msg)))
	if len(bufRead) != len(msg) || bytes.Compare(bufRead, msg) != 0 {
		t.Errorf("Error when reading in PassiveSocket returned TCPSocket for Client. Wrong Message Received")
	}
	if errRead != nil {
		t.Errorf("Error when reading in PassiveSocket returned TCPSocket for Client: %v", errRead)
	}
}

func TestPassiveSocketLongRead(t *testing.T) {
	passiveSocket, err := NewPassiveTCPSocket("127.0.0.1:10005")
	if err != nil {
		t.Errorf("Thrown error on creation: %v", err)
	}
	returnChan := make(chan *TCPSocket)
	defer close(returnChan)
	go AcceptClient(passiveSocket, returnChan)
	conn, errConn := net.Dial("tcp", "127.0.0.1:10005")
	if errConn != nil {
		t.Errorf("Thrown error on conn creation: %v", errConn)
	}
	clientSocket := <-returnChan
	str := "A"
	for len(str) < 100000 {
		str += "A"
	}
	msg := []byte(str)
	accum := 0
	for accum < len(msg) {
		sizeWr, errWr := conn.Write(msg)
		if errWr != nil {
			t.Errorf("Error when writing in client socket: %v", errWr)
		}
		accum += sizeWr
	}
	if accum < len(msg) {
		t.Errorf("Error on writing in client socket. Wrong size.")
	}
	bufRead, errRead := clientSocket.Read(uint32(len(msg)))
	if len(bufRead) != len(msg) || bytes.Compare(bufRead, msg) != 0 {
		t.Errorf("Error when reading in PassiveSocket returned TCPSocket for Client. Wrong Message Received")
	}
	if errRead != nil {
		t.Errorf("Error when reading in PassiveSocket returned TCPSocket for Client: %v", errRead)
	}
}
