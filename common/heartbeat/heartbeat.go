package heartbeat

import (
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol/sockets"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"time"
)

func sendHeartbeat(address string, name string) {
	sock, err := communication.NewActiveTCPSocket(address)
	if err != nil {
		log.Errorf("HeartBeat Signal | Error conecting to send heartbeat to %v | Err: %v", address, err)
		return
	}
	sph := sockets.NewSocketProtocolHandler(sock)
	mapOfContainer := make(map[string][]byte)
	mapOfContainer[utils.ServiceName] = serializer.SerializeString(name)
	err = sph.Write(
		&dataStructures.Message{
			TypeMessage: dataStructures.HeartBeat,
			DynMaps:     []*dataStructures.DynamicMap{dataStructures.NewDynamicMap(mapOfContainer)},
		},
	)
	if err != nil {
		log.Errorf("HeartBeat Signal | Error sending heartbeat to %v | Err: %v", address, err)
	}
	sph.Close()
}

func heartBeatLoop(addressesHealthCheckers []string, containerName string, timePerHeartbeatInSeconds uint32, endSignal chan bool) {
	for {
		log.Debugf("HeartBeat Loop | Sending heartbeat...")
		for i := 0; i < len(addressesHealthCheckers); i++ {
			go sendHeartbeat(addressesHealthCheckers[i], containerName)
		}
		timeout := time.After(time.Duration(timePerHeartbeatInSeconds) * time.Second)
		select {
		case <-endSignal:
			log.Infof("HeartBeat Loop | Closing heartbeat goroutine")
			return
		case <-timeout:
			// Do nothing, just wait till next heartbeat
		}
	}
}
