package protocol

import (
	"encoding/binary"
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	"github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

const sizeOfLen = 4

type SocketProtocolHandler struct {
	sock       communication.TCPSocketInterface
	serializer *data_structures.Serializer
}

func NewSocketProtocolHandler(sock communication.TCPSocketInterface) *SocketProtocolHandler {
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

func (sph *SocketProtocolHandler) sendLength(msgBytes []byte) error {
	log.Infof("Sending length...")
	lengthOfBytes := len(msgBytes)
	log.Infof("Length to send is %v", lengthOfBytes)
	msgSize := sph.serializer.SerializeUint(uint32(lengthOfBytes))
	log.Infof("Msg size bytes: %v", msgSize)
	write, err := sph.sock.Write(msgSize)
	if err != nil {
		return err
	}
	if write < sizeOfLen {
		return fmt.Errorf("send wrote less than %v bytes, instead size sent was %v", sizeOfLen, write)
	}
	return nil
}

func (sph *SocketProtocolHandler) sendMessage(msgBytes []byte) error {
	log.Infof("Sending message of len: %v...", len(msgBytes))
	write, err := sph.sock.Write(msgBytes)
	if err != nil {
		return err
	}
	if write < len(msgBytes) {
		return fmt.Errorf("send wrote less than %v bytes, instead size sent was %v", len(msgBytes), write)
	}
	log.Infof("Sent message")
	return nil
}

func (sph *SocketProtocolHandler) Write(msg *data_structures.Message) error {
	bytesToSend := sph.serializer.SerializeMsg(msg)
	err := sph.sendLength(bytesToSend)
	if err != nil {
		return fmt.Errorf("sending length: %v", err)
	}
	err = sph.sendMessage(bytesToSend)
	if err != nil {
		return fmt.Errorf("sending message: %v", err)
	}
	return nil
}

func (sph *SocketProtocolHandler) Close() {
	utils.CloseSocketAndNotifyError(sph.sock)
}
