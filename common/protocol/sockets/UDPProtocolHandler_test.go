package sockets

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/stretchr/testify/assert"
	"testing"
)

func writeFromCliPH(t *testing.T, cliPH *UdpProtocolhandler, packageToSend *dataStructures.UDPPacket, packageFromSvr *dataStructures.UDPPacket) {
	err := cliPH.Write(packageToSend, nil)
	assert.Nilf(t, err, fmt.Sprintf("Should not have thrown error when writing UDP Packet in client. Error was :%v", err))
	packet, addr, err := cliPH.Read()
	assert.Nilf(t, err, fmt.Sprintf("Should not have thrown error when receiving UDP Packet in client. Error was :%v", err))
	assert.Nilf(t, addr, "Should not have received address when receiving in client.")
	assert.Equalf(t, packageFromSvr.NodeID, packet.NodeID, "Wrong Coordinator ID received in client")
	assert.Equalf(t, packageFromSvr.PacketType, packet.PacketType, "Wrong Packet Type received in client")
}

func TestBasicUsageOfProtocolHandler(t *testing.T) {
	udpSvr, _ := communication.NewUdpServer("127.0.0.1", 10020)
	udpCli, _ := communication.NewUdpClient("127.0.0.1:10020")
	svrPH := NewUDPProtocolHandler(udpSvr)
	cliPH := NewUDPProtocolHandler(udpCli)
	packageSent := dataStructures.UDPPacket{PacketType: dataStructures.Coordinator, NodeID: 0}
	packageFromSvr := dataStructures.UDPPacket{PacketType: dataStructures.Election, NodeID: 0}
	go writeFromCliPH(t, cliPH, &packageSent, &packageFromSvr)
	packet, addr, err := svrPH.Read()
	assert.Equalf(t, packageSent.NodeID, packet.NodeID, "Coordinator ID in packet received is not equal to the sent one")
	assert.Equalf(t, packageSent.PacketType, packet.PacketType, "Packet Type in packet received is not equal to the sent one")
	assert.Nilf(t, err, fmt.Sprintf("Should not have thrown error when receiving UDP Packet in server. Error was :%v", err))
	assert.NotNilf(t, addr, "Address should not be nil as it was received in the server.")

	err = svrPH.Write(&packageFromSvr, addr)
	assert.Nilf(t, err, fmt.Sprintf("Should not have thrown error when sending UDP Packet in server. Error was :%v", err))

	cliPH.Close()
	svrPH.Close()
}
