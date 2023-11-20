package leader

import (
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
	"time"
)

func TestReceivingElectionFromMinorIDShouldRetransmitToGreaterID(t *testing.T) {
	addressMap := make(map[uint8]*net.UDPAddr)
	addressMap[2] = &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 50012}
	addressMap[3] = &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 50013}
	leaderService1 := NewLeaderElectionService(1, addressMap, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 50011})
	defer leaderService1.Close()

	addressMap2 := make(map[uint8]*net.UDPAddr)
	addressMap2[1] = &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 50011}
	addressMap2[3] = &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 50013}
	leaderService2 := NewLeaderElectionService(2, addressMap2, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 50012})
	defer leaderService2.Close()

	addressMap3 := make(map[uint8]*net.UDPAddr)
	addressMap3[1] = &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 50011}
	addressMap3[2] = &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 50012}
	leaderService3 := NewLeaderElectionService(3, addressMap3, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 50013})
	defer leaderService3.Close()

	go leaderService1.ReceiveNetMessages()
	go leaderService2.ReceiveNetMessages()
	go leaderService3.ReceiveNetMessages()

	time.Sleep(2 * time.Second)

	assert.Falsef(t, leaderService1.AmILeader(), "LeaderService1 should not be leader")
	assert.Falsef(t, leaderService2.AmILeader(), "LeaderService2 should not be leader")
	assert.Truef(t, leaderService3.AmILeader(), "LeaderService3 should be leader")
}

func TestShouldProclaimItselfAsTheLeaderIfItIsTheOnlyNode(t *testing.T) {
	addressMap := make(map[uint8]*net.UDPAddr)
	addressMap[2] = &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 50022}
	addressMap[3] = &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 50023}
	leaderService1 := NewLeaderElectionService(1, addressMap, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 50021})
	defer leaderService1.Close()

	go leaderService1.ReceiveNetMessages()

	time.Sleep(5 * time.Second)

	assert.Truef(t, leaderService1.AmILeader(), "LeaderService1 should be leader")
}

func TestLeaderFallsDownAndNewLeaderIsElected(t *testing.T) {
	addressMap := make(map[uint8]*net.UDPAddr)
	addressMap[2] = &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 50042}
	addressMap[3] = &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 50043}
	leaderService1 := NewLeaderElectionService(1, addressMap, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 50041})
	defer leaderService1.Close()

	addressMap2 := make(map[uint8]*net.UDPAddr)
	addressMap2[1] = &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 50041}
	addressMap2[3] = &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 50043}
	leaderService2 := NewLeaderElectionService(2, addressMap2, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 50042})
	defer leaderService2.Close()

	addressMap3 := make(map[uint8]*net.UDPAddr)
	addressMap3[1] = &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 50041}
	addressMap3[2] = &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 50042}
	leaderService3 := NewLeaderElectionService(3, addressMap3, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 50043})

	go leaderService1.ReceiveNetMessages()
	go leaderService2.ReceiveNetMessages()
	go leaderService3.ReceiveNetMessages()

	time.Sleep(2 * time.Second)

	assert.Falsef(t, leaderService1.AmILeader(), "LeaderService1 should not be leader")
	assert.Falsef(t, leaderService2.AmILeader(), "LeaderService2 should not be leader")
	assert.Truef(t, leaderService3.AmILeader(), "LeaderService3 should be leader")
	leaderService3.Close()

	time.Sleep(5 * time.Second)

	assert.Falsef(t, leaderService1.AmILeader(), "LeaderService1 should not be the new leader")
	assert.Truef(t, leaderService2.AmILeader(), "LeaderService2 should be the new leader")

}
