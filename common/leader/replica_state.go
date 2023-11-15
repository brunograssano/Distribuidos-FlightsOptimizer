package leader

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol/sockets"
	log "github.com/sirupsen/logrus"
	"net"
)

type ReplicaState struct {
	udpPHToLeader *sockets.UdpProtocolhandler
	nodeID        uint8
}

func NewReplicaState(leaderAddress *net.UDPAddr, nodeId uint8) (*ReplicaState, error) {
	udpCli, err := communication.NewUdpClient(fmt.Sprintf("%v:%v", leaderAddress.IP.String(), leaderAddress.Port))
	if err != nil {
		log.Errorf("ReplicaState | Error instantiating udp Client | %v", err)
		return nil, err
	}
	rs := ReplicaState{
		udpPHToLeader: sockets.NewUDPProtocolHandler(udpCli),
		nodeID:        nodeId,
	}
	return &rs, nil
}

func (rs *ReplicaState) AmILeader() bool {
	return false
}

func (rs *ReplicaState) Close() {
	rs.udpPHToLeader.Close()
}
