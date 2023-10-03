package protocol

import (
	"encoding/binary"
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	"github.com/brunograssano/Distribuidos-TP1/common/data_structures"
)

const sizeOfLen = 4

type SocketProtocolHandler struct {
	sock       *communication.TCPSocket
	serializer *data_structures.Serializer
}

func NewSocketProtocolHandler(sock *communication.TCPSocket) *SocketProtocolHandler {
	return &SocketProtocolHandler{
		sock:       sock,
		serializer: data_structures.NewSerializer(),
	}
}

func (sph *SocketProtocolHandler) getLengthOfMessage() (int, error) {
	bytesLen, err := sph.sock.Read(sizeOfLen)
	if err != nil {
		return -1, fmt.Errorf("Error receiving message size:  %v.", err)
	}
	return int(binary.BigEndian.Uint32(bytesLen)), nil
}

func (sph *SocketProtocolHandler) receiveMessage(length int) (*data_structures.Message, error) {
	msg, err := sph.sock.Read(uint32(length))
	if err != nil {
		return nil, fmt.Errorf("error receiving message of length %v: %v. Skipping client", length, err)
	}
	return sph.serializer.DeserializeMsg(msg), nil
}

func (sph *SocketProtocolHandler) Read() (*data_structures.Message, error) {
	length, err := sph.getLengthOfMessage()
	if err != nil {
		return nil, err
	}
	message, err := sph.receiveMessage(length)
	if err != nil {
		return nil, err
	}
	return message, nil
}
