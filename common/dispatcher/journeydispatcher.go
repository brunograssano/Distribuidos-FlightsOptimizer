package dispatcher

import (
	"crypto/sha256"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	log "github.com/sirupsen/logrus"
	"math/big"
)

// JourneyDispatcher Struct that dispatches journey messages
type JourneyDispatcher struct {
	channels []protocol.ProducerProtocolInterface
	input    protocol.ConsumerProtocolInterface
}

// NewJourneyDispatcher Creates a new dispatcher
func NewJourneyDispatcher(input protocol.ConsumerProtocolInterface, outputChannels []protocol.ProducerProtocolInterface) *JourneyDispatcher {
	return &JourneyDispatcher{
		input:    input,
		channels: outputChannels,
	}
}

// DispatchLoop Listens to the input queue and dispatches the msg to a Journey Saver
func (jd *JourneyDispatcher) DispatchLoop() {
	log.Infof("JourneyDispatcher | Started Journey Dispatcher loop")
	for {
		msg, ok := jd.input.Pop()
		if !ok {
			log.Infof("JourneyDispatcher | Input queue closed, stopping...")
			return
		}
		log.Debugf("JourneyDispatcher | Received message, dispatching its rows to Journey Savers")
		jd.dispatch(msg)
	}
}

// dispatch Executes the msg dispatch by calculating the sha256 hash
// on the starting airport and destination airport on each row of the message
func (jd *JourneyDispatcher) dispatch(message *dataStructures.Message) {
	for _, row := range message.DynMaps {
		startingAirport, err := row.GetAsBytes("startingAirport")
		if err != nil {
			log.Errorf("JourneyDispatcher | Error getting starting airport. Skipping row | %v", err)
			continue
		}
		destAirport, err := row.GetAsBytes("destinationAirport")
		if err != nil {
			log.Errorf("JourneyDispatcher | Error getting destination airport. Skipping row | %v", err)
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
