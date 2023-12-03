package client

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/getters"
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
	for i := 0; i < 4; i++ {
		log.Infof("----- Init results of ex %v -----", i+1)
		row := 0
		sendWithReconnection(conn, getters.GetExerciseMessageWithRow(uuid, i+1, row), nil)
		for {
			msg, err := conn.Read()
			if err != nil {
				log.Errorf("Results printer | Error reading results | %v", err)
				msgToReconnectWith := getters.GetExerciseMessageWithRow(uuid, i+1, row)
				sendWithReconnection(conn, msgToReconnectWith, nil)
				continue
			}
			if msg.TypeMessage == dataStructures.EOFGetter {
				break
			}
			printResults(msg.DynMaps)
			row += len(msg.DynMaps)
		}
		log.Infof("----- End results of ex %v -----", i+1)
	}
}
