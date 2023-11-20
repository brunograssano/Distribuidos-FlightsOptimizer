package sockets

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	log "github.com/sirupsen/logrus"
	"net"
)

const MaxRetriesUdp = 3

type UdpProtocolhandler struct {
	udpSocket communication.UDPSocket
}

func NewUDPProtocolHandler(socket communication.UDPSocket) *UdpProtocolhandler {
	return &UdpProtocolhandler{
		udpSocket: socket,
	}
}

func (uph *UdpProtocolhandler) constructACK() []byte {
	packet := dataStructures.UDPPacket{PacketType: dataStructures.ACK}
	return serializer.SerializeUDPPacket(&packet)
}

func (uph *UdpProtocolhandler) isAck(bytesPacket []byte) bool {
	packet := serializer.DeserializeUDPPacket(bytesPacket)
	return packet.PacketType == dataStructures.ACK
}

func (uph *UdpProtocolhandler) Read() (*dataStructures.UDPPacket, *net.UDPAddr, error) {
	bytesPacket, address, err := uph.udpSocket.Receive(dataStructures.SizeUdpPacket)
	if err != nil {
		log.Errorf("UDPProtocolHandler | Error reading | Address: %v | Error: %v", address, err)
		return nil, nil, err
	}
	_, err = uph.udpSocket.Send(uph.constructACK(), address)
	if err != nil {
		log.Warnf("UDPProtocolHandler | Error sending ACK | Address: %v | Error: %v", address, err)
		// Should ignore the message because ACK did not reach the sender. Sender will retry eventually...
		return nil, nil, err
	}
	return serializer.DeserializeUDPPacket(bytesPacket), address, nil
}

func (uph *UdpProtocolhandler) waitForAck(addrAwaited *net.UDPAddr) bool {
	packetBytes, address, err := uph.udpSocket.Receive(dataStructures.SizeUdpPacket)
	if err != nil {
		if netErr, ok := err.(net.Error); !(ok && netErr.Timeout()) {
			log.Errorf("UDPProtocolHandler | Error awaiting for ACK | Address: %v | Error: %v", address, err)
		}
		return false
	}
	return isSameAddress(addrAwaited, address) && uph.isAck(packetBytes)
}

func isSameAddress(addrAwaited *net.UDPAddr, address *net.UDPAddr) bool {
	return address == addrAwaited || (address.Port == addrAwaited.Port && address.IP.String() == addrAwaited.IP.String())
}

// Write: Writes an udp packet into the specified address. The
//
//	Address is nullable if udp socket is of type client and is already connected to the server
func (uph *UdpProtocolhandler) Write(packet *dataStructures.UDPPacket, address *net.UDPAddr) error {
	receivedACK := false
	currentRetry := 0
	for !receivedACK && currentRetry < MaxRetriesUdp {
		_, err := uph.udpSocket.Send(serializer.SerializeUDPPacket(packet), address)
		if err != nil {
			log.Errorf("UDPProtocolHandler | Error Writing | Address: %v | Error: %v", address, err)
			return err
		}
		receivedACK = uph.waitForAck(address)
		if !receivedACK {
			currentRetry++
		}
	}
	if currentRetry == MaxRetriesUdp && !receivedACK {
		log.Errorf("UDPProtocolHandler | Could not send a packet | Retried %v times unsuccesfully | ACK was not received", currentRetry)
		return fmt.Errorf("could not send the udp packet. Ack's have not arrived to the host")
	}
	return nil
}

func (uph *UdpProtocolhandler) Close() {
	uph.udpSocket.Close()
}
