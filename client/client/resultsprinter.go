package client

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	socketsProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/sockets"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"strings"
)

func printResults(dynMaps []*dataStructures.DynamicMap) {
	for _, row := range dynMaps {
		line := strings.TrimRight(serializer.SerializeToString(row), utils.NewLine)
		log.Infof(line)
	}

}

func RequestResults(uuid string, conn *socketsProtocol.SocketProtocolHandler) {
	log.Infof("Results printer | Requesting results")
	msg := &dataStructures.Message{TypeMessage: dataStructures.GetResults, ClientId: uuid}
	err := conn.Write(msg)
	if err != nil {
		log.Errorf("Results printer | Error requesting results | %v", err)
		return
	}

	for i := 0; i < 4; i++ {
		log.Infof("----- Init results of ex %v -----", i+1)
		for {
			msg, err = conn.Read()
			if err != nil {
				log.Errorf("Results printer | Error reading results | %v", err)
				return
			}
			if msg.TypeMessage == dataStructures.EOFGetter {
				break
			}
			printResults(msg.DynMaps)
		}
		log.Infof("----- End results of ex %v -----", i+1)
	}
}
