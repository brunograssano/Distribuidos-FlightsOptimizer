package dispatcher

import (
	"crypto/sha256"
	"github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	log "github.com/sirupsen/logrus"
	"math/big"
)

type JourneyDispatcher struct {
	channels []protocol.ProducerProtocolInterface
	input    protocol.ConsumerProtocolInterface
}

func NewJourneyDispatcher(input protocol.ConsumerProtocolInterface, outputChannels []protocol.ProducerProtocolInterface) *JourneyDispatcher {
	return &JourneyDispatcher{
		input:    input,
		channels: outputChannels,
	}
}

func (jd *JourneyDispatcher) Dispatch(message *data_structures.Message) {
	for _, row := range message.DynMaps {
		startingAirport, err := row.GetAsBytes("startingAirport")
		if err != nil {
			log.Errorf("Error getting starting airport. Skipping row...")
			continue
		}
		destAirport, err := row.GetAsBytes("destinationAirport")
		if err != nil {
			log.Errorf("Error getting destination airport. Skipping row...")
			continue
		}
		var bytesToHash []byte
		bytesToHash = append(bytesToHash, startingAirport...)
		bytesToHash = append(bytesToHash, destAirport...)

		hasher := sha256.New()
		hasher.Write(bytesToHash)
		hashResult := hasher.Sum(nil)
		hashInt := new(big.Int)
		hashInt.SetBytes(hashResult)
		resultIndex := int(hashInt.Int64()) % len(jd.channels)
		err = jd.channels[resultIndex].Send(message)
		if err != nil {
			log.Errorf("Error sending message to queue #%v. Skipping row...", resultIndex)
		}
	}

}
