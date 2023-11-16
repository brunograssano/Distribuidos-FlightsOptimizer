package leader

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol/sockets"
	log "github.com/sirupsen/logrus"
	"net"
)

type LeaderElectionService struct {
	currentState     BullyState
	id               uint8
	address          *net.UDPAddr
	leaderID         uint8
	listener         *sockets.UdpProtocolhandler
	netAddresses     map[uint8]*net.UDPAddr
	netClientSockets map[uint8]*sockets.UdpProtocolhandler
	leaderDown       chan bool
}

func NewLeaderElectionService(id uint8, networkNodes map[uint8]*net.UDPAddr, myAddr *net.UDPAddr) *LeaderElectionService {
	netClientSockets := make(map[uint8]*sockets.UdpProtocolhandler)
	listenerSocketUdp, err := communication.NewUdpServer(myAddr.IP.String(), myAddr.Port)
	if err != nil {
		log.Fatalf("LeaderElectionService %v | Error trying to create UDP Server Socket | %v", id, err)
	}
	for idNode, udpAddr := range networkNodes {
		udpCli, err := communication.NewUdpClient(fmt.Sprintf("%v:%v", udpAddr.IP.String(), udpAddr.Port))
		if err != nil {
			log.Errorf("LeaderElectionService | Error trying to create UDP Client Socket | %v", err)
		}
		cliPH := sockets.NewUDPProtocolHandler(udpCli)
		netClientSockets[idNode] = cliPH
	}
	lisPH := sockets.NewUDPProtocolHandler(listenerSocketUdp)
	les := LeaderElectionService{
		netAddresses:     networkNodes,
		address:          myAddr,
		id:               id,
		leaderID:         id,
		listener:         lisPH,
		netClientSockets: netClientSockets,
		leaderDown:       make(chan bool, 1),
	}
	return &les
}

func (les *LeaderElectionService) startElectionOnLeaderDown() {
	for {
		isLeaderDown := <-les.leaderDown
		if !isLeaderDown {
			log.Infof("LeaderElectionService #%v | Exiting goroutine that starts new election on leader down detection", les.id)
			return
		}
		log.Infof("LeaderElectionService #%v | Detected the leader is down, starting election", les.id)
		if les.currentState != nil {
			les.currentState.Close()
			les.currentState = nil
		}
		les.startElection()
	}
}

func (les *LeaderElectionService) ReceiveNetMessages() {
	go les.startElectionOnLeaderDown()
	go les.startElection()
	for {
		udpPacket, _, err := les.listener.Read()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			log.Errorf("LeaderElectionService #%v | Error receiving UDP Packets | %v | Now Closing...", les.id, err)
			return
		}
		if udpPacket.PacketType == dataStructures.Election && udpPacket.NodeID < les.id {
			les.startElection()
		}
		if udpPacket.PacketType == dataStructures.Coordinator {
			les.handleCoordinatorMessage(udpPacket)
		}
	}
}

func (les *LeaderElectionService) handleCoordinatorMessage(udpPacket *dataStructures.UDPPacket) {
	les.leaderID = udpPacket.NodeID
	log.Infof("LeaderElectionService #%v | Received New Coordinator: #%v | Now changing current state...", les.id, udpPacket.NodeID)
	if les.currentState != nil {
		les.currentState.Close()
	}
	rs, err := NewReplicaState(les.netAddresses[udpPacket.NodeID], les.id, les.leaderDown)
	if err != nil {
		log.Fatalf("LeaderElectionService #%v | Error creating replica state | %v | Now Closing...", les.id, err)
	}
	go rs.startLeaderHealthChecking()
	les.currentState = rs
}

func (les *LeaderElectionService) broadcastCoordinator() {
	les.leaderID = les.id
	if les.currentState != nil {
		les.currentState.Close()
	}
	les.currentState = NewLeaderState()
	udpPacket := dataStructures.UDPPacket{PacketType: dataStructures.Coordinator, NodeID: les.id}
	for idNode, cliPH := range les.netClientSockets {
		if les.id > idNode {
			go les.sendCoordinatorToClient(idNode, cliPH, udpPacket)
		}
	}
}

func (les *LeaderElectionService) sendCoordinatorToClient(idNode uint8, cliPH *sockets.UdpProtocolhandler, udpPacket dataStructures.UDPPacket) {
	log.Infof("LeaderElectionService #%v | Sending Coordinator to the node %v", les.id, idNode)
	err := cliPH.Write(&udpPacket, nil)
	if err != nil {
		log.Errorf("LeaderElectionService #%v | Error sending coordinator to the node %v | %v", les.id, idNode, err)
	}
}

func (les *LeaderElectionService) startElection() {
	udpPacket := dataStructures.UDPPacket{PacketType: dataStructures.Election, NodeID: les.id}
	resultsChan := make(chan bool, len(les.netClientSockets))
	quantity := 0
	for idNode, cliPH := range les.netClientSockets {
		if idNode > les.id {
			go les.sendElection(idNode, cliPH, udpPacket, resultsChan)
			quantity++
		}
	}
	isLeader := true
	for i := 0; i < quantity; i++ {
		isLeader = isLeader && <-resultsChan
	}
	if isLeader {
		les.broadcastCoordinator()
	}
}

func (les *LeaderElectionService) sendElection(idNode uint8, cliPH *sockets.UdpProtocolhandler, udpPacket dataStructures.UDPPacket, receivedACKFromGreaterOne chan bool) {
	err := cliPH.Write(&udpPacket, nil)
	result := false
	if err != nil {
		log.Errorf("LeaderElectionService #%v | Error sending election to the node %v | %v", les.id, idNode, err)
		result = true
	}
	receivedACKFromGreaterOne <- result
}

func (les *LeaderElectionService) AmILeader() bool {
	if les.currentState == nil {
		return false
	}
	return les.currentState.AmILeader()
}

func (les *LeaderElectionService) Close() {
	log.Infof("LeaderElectionService #%v | Closing resources...", les.id)
	les.listener.Close()
	if les.currentState != nil {
		les.currentState.Close()
	}
	les.leaderDown <- false
	close(les.leaderDown)
	for _, cliPH := range les.netClientSockets {
		cliPH.Close()
	}
}
