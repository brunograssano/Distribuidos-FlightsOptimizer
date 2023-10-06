package main

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	log "github.com/sirupsen/logrus"
)

func printResults(dynMaps []*dataStructures.DynamicMap) {
	serializer := dataStructures.NewSerializer()
	for _, row := range dynMaps {
		line := serializer.SerializeToString(row)
		log.Infof(line)
	}

}

func RequestResults(err error, conn *protocol.SocketProtocolHandler) {
	log.Infof("Requesting results")
	msg := &dataStructures.Message{TypeMessage: dataStructures.GetResults}
	err = conn.Write(msg)
	if err != nil {
		log.Errorf("Error requesting results: %v", err)
		return
	}

	for i := 0; i < 4; {
		msg, err = conn.Read()
		if err != nil {
			log.Errorf("Error reading results: %v", err)
			return
		}
		if msg.TypeMessage == dataStructures.EOFGetter {
			i++
			log.Infof("----- End results of ex %v -----", i)
			continue
		}

		printResults(msg.DynMaps)
	}
}
