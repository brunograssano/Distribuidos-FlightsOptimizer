package protocol

import (
	"fmt"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	log "github.com/sirupsen/logrus"
)

func sendEOFToOutput(localSent int, sent int, prodOutputQueue ProducerProtocolInterface) error {
	serializer := dataStructures.NewSerializer()
	dynMapData := make(map[string][]byte)
	dynMapData["localReceived"] = serializer.SerializeUint(uint32(0))
	dynMapData["localSent"] = serializer.SerializeUint(uint32(0))
	dynMapData["prevSent"] = serializer.SerializeUint(uint32(localSent + sent))
	err := prodOutputQueue.Send(&dataStructures.Message{
		TypeMessage: dataStructures.EOFFlightRows,
		DynMaps:     []*dataStructures.DynamicMap{dataStructures.NewDynamicMap(dynMapData)},
	})
	return err
}

func sendEOFToInput(localReceived int, received int, prevSent int, sent int, localSent int, prodInputQueue ProducerProtocolInterface) error {
	serializer := dataStructures.NewSerializer()
	dynMapData := make(map[string][]byte)
	dynMapData["localReceived"] = serializer.SerializeUint(uint32(localReceived + received))
	dynMapData["localSent"] = serializer.SerializeUint(uint32(sent + localSent))
	dynMapData["prevSent"] = serializer.SerializeUint(uint32(prevSent))
	err := prodInputQueue.Send(&dataStructures.Message{
		TypeMessage: dataStructures.EOFFlightRows,
		DynMaps:     []*dataStructures.DynamicMap{dataStructures.NewDynamicMap(dynMapData)},
	})
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
	sent := prodOutputQueues[0].GetSentMessages()
	received := consInputQueue.GetReceivedMessages()
	// We get the total sent messages from the EOF queue, the total that were processed by the controllers,
	// and the total sent by this controller to the next step
	// "prevSent", "localReceived", "localSent"
	prevSent, err := message.DynMaps[0].GetAsInt("prevSent")
	if err != nil {
		log.Errorf("%v", err)
		return err
	}
	localReceived, err := message.DynMaps[0].GetAsInt("localReceived")
	if err != nil {
		log.Errorf("%v", err)
		return err
	}
	localSent, err := message.DynMaps[0].GetAsInt("localSent")
	if err != nil {
		log.Errorf("%v", err)
		return err
	}
	if received+localReceived >= prevSent {
		for i := 0; i < len(prodOutputQueues); i++ {
			err = sendEOFToOutput(localSent, sent, prodOutputQueues[i])
			if err != nil {
				log.Errorf("%v", err)
				return err
			}
		}
	}
	return sendEOFToInput(localReceived, received, prevSent, sent, localSent, prodInputQueue)
}
