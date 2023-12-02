package queues

import (
	"fmt"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"strings"
)

func sendEOFToOutput(prodOutputQueue ProducerProtocolInterface, message *dataStructures.Message, rowId int) error {
	dynMapData := make(map[string][]byte)
	dynMapData[utils.NodesVisited] = serializer.SerializeString("")
	err := prodOutputQueue.Send(&dataStructures.Message{
		TypeMessage: dataStructures.EOFFlightRows,
		DynMaps:     []*dataStructures.DynamicMap{dataStructures.NewDynamicMap(dynMapData)},
		ClientId:    message.ClientId,
		MessageId:   message.MessageId,
		RowId:       uint16(rowId),
	})
	return err
}

func sendEOFToInput(prodInputQueue ProducerProtocolInterface, message *dataStructures.Message, nodes string) error {
	dynMapData := make(map[string][]byte)
	dynMapData[utils.NodesVisited] = serializer.SerializeString(nodes)
	err := prodInputQueue.Send(&dataStructures.Message{
		TypeMessage: dataStructures.EOFFlightRows,
		DynMaps:     []*dataStructures.DynamicMap{dataStructures.NewDynamicMap(dynMapData)},
		ClientId:    message.ClientId,
		MessageId:   message.MessageId,
		//Mutates the row id to avoid discarding before it is correctly handled
		RowId: message.RowId + 1,
	})
	return err
}

// HandleEOF Function that handles the EOF message, decides if it sends the message to the consumed queue or passes it to the next step
func HandleEOF(
	message *dataStructures.Message,
	prodInputQueue ProducerProtocolInterface,
	prodOutputQueues []ProducerProtocolInterface,
	nodeId string,
	quantityOfEOF uint,
) error {
	if message.TypeMessage != dataStructures.EOFFlightRows {
		return fmt.Errorf("type is not EOF")
	}

	nodes, err := message.DynMaps[0].GetAsString(utils.NodesVisited)
	if err != nil {
		log.Errorf("EOFHandler %v | Error getting nodes visited | %v", nodeId, err)
		return err
	}

	separator := utils.CommaSeparator
	if nodes == "" {
		separator = ""
	}
	if !amIInArray(nodes, nodeId) {
		nodes = fmt.Sprintf("%v%v%v", nodes, separator, nodeId)
	}
	if len(strings.Split(nodes, utils.CommaSeparator)) == int(quantityOfEOF) {
		log.Infof("EOF Handler %v | Sending EOF to next services...", nodeId)
		for idx, outQueue := range prodOutputQueues {
			err = sendEOFToOutput(outQueue, message, idx)
			if err != nil {
				log.Errorf("EOF Handler %v | Error sending EOF", nodeId)
				return err
			}
		}
		return nil
	}
	log.Infof("EOF Handler %v | Enqueueing EOF again...", nodeId)
	return sendEOFToInput(prodInputQueue, message, nodes)
}

func amIInArray(nodes string, nodeId string) bool {
	nodesArray := strings.Split(nodes, utils.CommaSeparator)
	inArray := false
	for _, node := range nodesArray {
		if node == nodeId {
			inArray = true
			break
		}
	}
	return inArray
}
