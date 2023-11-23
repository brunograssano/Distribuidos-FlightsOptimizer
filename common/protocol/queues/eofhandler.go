package queues

import (
	"fmt"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

func sendEOFToOutput(localSent int, sent int, prodOutputQueue ProducerProtocolInterface, message *dataStructures.Message, idx int) error {
	dynMapData := make(map[string][]byte)
	dynMapData[utils.LocalReceived] = serializer.SerializeUint(uint32(0))
	dynMapData[utils.LocalSent] = serializer.SerializeUint(uint32(0))
	dynMapData[utils.PrevSent] = serializer.SerializeUint(uint32(localSent + sent))
	log.Infof("EOF Handler | Sent length of EOF is: %v. Local sent received was: %v, and node sent is: %v", localSent+sent, localSent, sent)
	err := prodOutputQueue.Send(&dataStructures.Message{
		TypeMessage: dataStructures.EOFFlightRows,
		DynMaps:     []*dataStructures.DynamicMap{dataStructures.NewDynamicMap(dynMapData)},
		ClientId:    message.ClientId,
		MessageId:   message.MessageId,
		RowId:       uint16(idx),
	})
	prodOutputQueue.ClearData(message.ClientId)
	return err
}

func sendEOFToInput(localReceived int, received int, prevSent int, sent int, localSent int, prodInputQueue ProducerProtocolInterface, message *dataStructures.Message) error {
	dynMapData := make(map[string][]byte)
	dynMapData[utils.LocalReceived] = serializer.SerializeUint(uint32(localReceived + received))
	dynMapData[utils.LocalSent] = serializer.SerializeUint(uint32(sent + localSent))
	dynMapData[utils.PrevSent] = serializer.SerializeUint(uint32(prevSent))
	err := prodInputQueue.Send(&dataStructures.Message{
		TypeMessage: dataStructures.EOFFlightRows,
		DynMaps:     []*dataStructures.DynamicMap{dataStructures.NewDynamicMap(dynMapData)},
		ClientId:    message.ClientId,
		MessageId:   message.MessageId,
		//Mutates the row id to avoid discarding before it is correctly handled
		RowId: message.RowId + 1,
	})
	prodInputQueue.ClearData(message.ClientId)
	return err
}

// HandleEOF Function that handles the EOF message, decides if it sends the message to the consumed queue or passes it to the next step
func HandleEOF(
	message *dataStructures.Message,
	consInputQueue ConsumerProtocolInterface,
	prodInputQueue ProducerProtocolInterface,
	prodOutputQueues []ProducerProtocolInterface,
) error {
	if message.TypeMessage != dataStructures.EOFFlightRows {
		return fmt.Errorf("type is not EOF")
	}
	// Zero is arbitrary for any case... Array of producers should have sent the same amount for every listener.
	sent := prodOutputQueues[0].GetSentMessages(message.ClientId)
	received := consInputQueue.GetReceivedMessages(message.ClientId)
	consInputQueue.ClearData(message.ClientId)
	for _, prodOQ := range prodOutputQueues {
		prodOQ.ClearData(message.ClientId)
	}
	// We get the total sent messages from the EOF queue, the total that were processed by the controllers,
	// and the total sent by this controller to the next step
	// "prevSent", "localReceived", "localSent"
	prevSent, err := message.DynMaps[0].GetAsInt(utils.PrevSent)
	if err != nil {
		log.Errorf("EOFHandler | Error getting prevSent | %v", err)
		return err
	}
	localReceived, err := message.DynMaps[0].GetAsInt(utils.LocalReceived)
	if err != nil {
		log.Errorf("EOFHandler | Error getting localReceived | %v", err)
		return err
	}
	localSent, err := message.DynMaps[0].GetAsInt(utils.LocalSent)
	if err != nil {
		log.Errorf("EOFHandler | Error getting localSent | %v", err)
		return err
	}
	if received+localReceived >= prevSent {
		log.Infof("EOF Handler | Received accumulated were: %v. Prev sent were: %v", received+localReceived, prevSent)
		log.Infof("EOF Handler | Sum of EOF reached the expected value. Sending EOF to next nodes...")
		for i := 0; i < len(prodOutputQueues); i++ {
			log.Infof("EOF Handler | Sending EOF to Next node with index %v", i)
			err = sendEOFToOutput(localSent, sent, prodOutputQueues[i], message, i)
			if err != nil {
				log.Errorf("EOFHandler | Error sending EOF to Output | %v", err)
				return err
			}
		}
		return nil
	}
	log.Infof("EOF Handler | Received accumulated were: %v. Prev sent were: %v", received+localReceived, prevSent)
	log.Infof("EOF Handler | Enqueueing EOF again...")
	return sendEOFToInput(localReceived, received, prevSent, sent, localSent, prodInputQueue, message)
}
