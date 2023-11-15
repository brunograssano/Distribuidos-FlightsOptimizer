package leader

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol/sockets"
	log "github.com/sirupsen/logrus"
	"net"
)

type BullyLeader struct {
	currentState     BullyState
	id               uint8
	address          *net.UDPAddr
	leaderID         uint8
	listener         *sockets.UdpProtocolhandler
	netAddresses     map[uint8]*net.UDPAddr
	netClientSockets map[uint8]*sockets.UdpProtocolhandler
}

func getMaxValueFromNetworkMap(netMap map[uint8]*net.UDPAddr) uint8 {
	maxVal := uint8(0)
	for key, _ := range netMap {
		if maxVal < key {
			maxVal = key
		}
	}
	return maxVal
}

func NewBullyLeader(id uint8, networkNodes map[uint8]*net.UDPAddr, myAddr *net.UDPAddr) *BullyLeader {
	maxId := getMaxValueFromNetworkMap(networkNodes)
	netClientSockets := make(map[uint8]*sockets.UdpProtocolhandler)
	for idNode, udpAddr := range networkNodes {
		udpCli, err := communication.NewUdpClient(fmt.Sprintf("%v:%v", udpAddr.IP.String(), udpAddr.Port))
		if err != nil {
			log.Errorf("BullyLeader | Error trying to create UDP Client Socket | %v", err)
		}
		cliPH := sockets.NewUDPProtocolHandler(udpCli)
		netClientSockets[idNode] = cliPH
	}
	listenerSocketUdp, err := communication.NewUdpServer(myAddr.IP.String(), myAddr.Port)
	if err != nil {
		log.Errorf("BullyLeader | Error trying to create UDP Server Socket | %v", err)
	}
	lisPH := sockets.NewUDPProtocolHandler(listenerSocketUdp)
	bl := BullyLeader{
		netAddresses:     networkNodes,
		address:          myAddr,
		id:               id,
		leaderID:         id,
		listener:         lisPH,
		netClientSockets: netClientSockets,
	}
	go bl.receiveNetMessages()
	if id > maxId {
		bl.broadcastCoordinator()
	} else {
		bl.currentState = nil
		bl.startElection()
	}
	return &bl
}

func (bl *BullyLeader) receiveNetMessages() {
	udpPacket, _, err := bl.listener.Read()
	if err != nil {
		log.Errorf("BullyLeader | Error receiving UDP Packets | Now Closing...")
		bl.Close()
	}
	if udpPacket.PacketType == dataStructures.Election && udpPacket.NodeID < bl.id {
		bl.startElection()
	}
	if udpPacket.PacketType == dataStructures.Coordinator {
		bl.leaderID = udpPacket.NodeID
	}
}

func (bl *BullyLeader) broadcastCoordinator() {
	udpPacket := dataStructures.UDPPacket{PacketType: dataStructures.Coordinator, NodeID: uint8(bl.id)}
	for idNode, cliPH := range bl.netClientSockets {
		err := cliPH.Write(&udpPacket, nil)
		if err != nil {
			log.Errorf("BullyLeader | Error sending coordinator to the node %v | %v", idNode, err)
		}
	}
	bl.leaderID = bl.id
	bl.currentState = NewLeaderState()
}

func (bl *BullyLeader) startElection() {
	udpPacket := dataStructures.UDPPacket{PacketType: dataStructures.Election, NodeID: uint8(bl.id)}
	receivedACKFromGreaterOne := false
	for idNode, cliPH := range bl.netClientSockets {
		if idNode > bl.id {
			err := cliPH.Write(&udpPacket, nil)
			if err != nil {
				log.Errorf("BullyLeader | Error sending election to the node %v | %v", idNode, err)
			} else {
				receivedACKFromGreaterOne = true
			}
		}
	}
	if !receivedACKFromGreaterOne {
		bl.broadcastCoordinator()
	}
}

func (bl *BullyLeader) Close() {
	log.Infof("BullyLeader | Closing resources...")
	bl.listener.Close()
	bl.currentState.Close()
	for _, cliPH := range bl.netClientSockets {
		cliPH.Close()
	}
}
