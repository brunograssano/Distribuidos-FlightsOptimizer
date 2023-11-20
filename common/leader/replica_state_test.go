package leader

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol/sockets"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestShouldSendThatTheLeaderIsDown(t *testing.T) {
	conn, err := communication.NewUdpClient("localhost:54673")
	assert.Nilf(t, err, fmt.Sprintf("Error Creating UDP Client: %v", err))
	leaderDown := make(chan bool, 1)
	rs := &ReplicaState{
		udpPHToLeader: sockets.NewUDPProtocolHandler(conn),
		nodeID:        1,
		leaderDown:    leaderDown,
	}
	defer rs.Close()
	go rs.startLeaderHealthChecking()
	select {
	case ld := <-leaderDown:
		assert.True(t, ld, "Leader Down is false and should be true.")
	case <-time.After(20 * time.Second):
		assert.Fail(t, "Should have sent notif by now...")
	}
}
