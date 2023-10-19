package communication

import (
	"bytes"
	"github.com/stretchr/testify/assert"
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
	assert.Nilf(t, err, "Thrown error on creation: %v", err)

	err = passiveSocket.Close()
	assert.Nilf(t, err, "Thrown error on close: %v", err)
}

func TestPassiveSocketThrowsErrorOnCreationWhenPortIsNotFree(t *testing.T) {
	passiveSocket, err := NewPassiveTCPSocket("127.0.0.1:10001")
	assert.Nilf(t, err, "Thrown error on creation: %v", err)

	_, errPort := NewPassiveTCPSocket("127.0.0.1:10001")
	assert.Error(t, errPort, "Should have thrown error on creation when port is used.")

	err = passiveSocket.Close()
	assert.Nilf(t, err, "Thrown error on close: %v", err)
}

func TestPassiveSocketShortWrite(t *testing.T) {
	passiveSocket, err := NewPassiveTCPSocket("127.0.0.1:10002")
	assert.Nilf(t, err, "Thrown error on creation: %v", err)

	returnChan := make(chan *TCPSocket)
	defer close(returnChan)
	go AcceptClient(passiveSocket, returnChan)
	conn, errConn := net.Dial("tcp", "127.0.0.1:10002")
	assert.Nilf(t, errConn, "Thrown error on conn creation: %v", errConn)

	clientSocket := <-returnChan
	msg := []byte("Message")
	lenWr, errWr := clientSocket.Write(msg)
	assert.Nilf(t, errWr, "Thrown error on write: %v", errWr)
	assert.Equalf(t, len(msg), lenWr, "Wrong length of write")

	buf := make([]byte, len(msg))
	sizeRead, errRead := conn.Read(buf)
	assert.True(t, sizeRead == len(msg) && bytes.Compare(buf, msg) == 0, "Wrong read.")
	assert.Nilf(t, errRead, "Error reading message in client socket")

	_ = conn.Close()
	_ = clientSocket.Close()
	_ = passiveSocket.Close()
}

func TestPassiveSocketLongWrite(t *testing.T) {
	passiveSocket, err := NewPassiveTCPSocket("127.0.0.1:10003")
	assert.Nilf(t, err, "Thrown error on creation: %v", err)

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
	assert.Nilf(t, errWr, "Thrown error on write: %v", errWr)
	assert.Equalf(t, len(msg), lenWr, "Wrong length of write")

	buf := make([]byte, len(msg))
	accum := 0
	for accum < len(msg) {
		sizeRead, errRead := conn.Read(buf[accum:])
		accum += sizeRead
		assert.Nilf(t, errRead, "Error reading message in client socket")
	}
	assert.True(t, accum == len(msg) && bytes.Compare(buf, msg) == 0, "Wrong read.")

	_ = conn.Close()
	_ = clientSocket.Close()
	_ = passiveSocket.Close()
}

func TestPassiveSocketShortRead(t *testing.T) {
	passiveSocket, err := NewPassiveTCPSocket("127.0.0.1:10004")
	assert.Nilf(t, err, "Thrown error on creation: %v", err)

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
	assert.Nilf(t, errWr, "Thrown error on write: %v", errWr)
	assert.Equalf(t, len(msg), sizeWr, "Wrong length of write")

	bufRead, errRead := clientSocket.Read(uint32(len(msg)))
	assert.True(t, len(bufRead) == len(msg) && bytes.Compare(bufRead, msg) == 0, "Error when reading in PassiveSocket returned TCPSocket for Client. Wrong Message Received")
	assert.Nilf(t, errRead, "Error when reading in PassiveSocket returned TCPSocket for Client: %v", errRead)
}

func TestPassiveSocketLongRead(t *testing.T) {
	passiveSocket, err := NewPassiveTCPSocket("127.0.0.1:10005")
	assert.Nilf(t, err, "Thrown error on creation: %v", err)

	returnChan := make(chan *TCPSocket)
	defer close(returnChan)
	go AcceptClient(passiveSocket, returnChan)
	conn, errConn := net.Dial("tcp", "127.0.0.1:10005")
	assert.Nilf(t, errConn, "Thrown error on conn creation: %v", errConn)

	clientSocket := <-returnChan
	str := "A"
	for len(str) < 100000 {
		str += "A"
	}
	msg := []byte(str)
	accum := 0
	for accum < len(msg) {
		sizeWr, errWr := conn.Write(msg)
		assert.Nilf(t, errWr, "Thrown error on write: %v", errWr)
		accum += sizeWr
	}
	assert.Equalf(t, len(msg), accum, "Error on writing in client socket. Wrong size.")

	bufRead, errRead := clientSocket.Read(uint32(len(msg)))
	assert.True(t, len(bufRead) == len(msg) && bytes.Compare(bufRead, msg) == 0, "Error when reading in PassiveSocket returned TCPSocket for Client. Wrong Message Received")
	assert.Nilf(t, errRead, "Error when reading in PassiveSocket returned TCPSocket for Client: %v", errRead)
}
