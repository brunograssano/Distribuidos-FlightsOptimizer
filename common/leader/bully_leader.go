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
	bl := BullyLeader{
		netAddresses:     networkNodes,
		address:          myAddr,
		id:               id,
		leaderID:         id,
		netClientSockets: netClientSockets,
	}
	if id > maxId {
		bl.currentState = NewLeaderState()
		bl.broadcastCoordinator()
	} else {
		bl.startElection()
		bl.currentState = nil
	}
	return &bl
}

func (bl *BullyLeader) broadcastCoordinator() {
	udpPacket := dataStructures.UDPPacket{PacketType: dataStructures.Coordinator, NodeID: uint8(bl.id)}
	for idNode, cliPH := range bl.netClientSockets {
		err := cliPH.Write(&udpPacket, nil)
		if err != nil {
			log.Errorf("BullyLeader | Error sending coordinator to the node %v | %v", idNode, err)
		}
	}
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
