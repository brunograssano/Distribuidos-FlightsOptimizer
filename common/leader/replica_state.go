package leader

import (
	"errors"
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol/sockets"
	log "github.com/sirupsen/logrus"
	"net"
	"time"
)

const healthCheckingSleep = 1

type ReplicaState struct {
	udpPHToLeader *sockets.UdpProtocolhandler
	nodeID        uint8
	leaderDown    chan bool
}

func NewReplicaState(leaderAddress *net.UDPAddr, nodeId uint8, leaderDown chan bool) (*ReplicaState, error) {
	udpCli, err := communication.NewUdpClient(fmt.Sprintf("%v:%v", leaderAddress.IP.String(), leaderAddress.Port))
	if err != nil {
		log.Errorf("ReplicaState | Error instantiating udp Client | %v", err)
		return nil, err
	}
	rs := ReplicaState{
		udpPHToLeader: sockets.NewUDPProtocolHandler(udpCli),
		nodeID:        nodeId,
		leaderDown:    leaderDown,
	}
	return &rs, nil
}

func (rs *ReplicaState) AmILeader() bool {
	return false
}

func (rs *ReplicaState) startLeaderHealthChecking() {
	for {
		err := rs.udpPHToLeader.Write(&dataStructures.UDPPacket{PacketType: dataStructures.HealthCheck, NodeID: rs.nodeID}, nil)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				log.Errorf("ReplicaState | Error trying to send health check to the leader | %v", err)
				rs.leaderDown <- true
			}
			return
		}
		time.Sleep(healthCheckingSleep * time.Second)
	}
}

func (rs *ReplicaState) Close() {
	rs.udpPHToLeader.Close()
}
