package dispatcher

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	queueProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"hash/fnv"
)

// JourneyDispatcher Struct that dispatches journey messages
type JourneyDispatcher struct {
	channels    []queueProtocol.ProducerProtocolInterface
	input       queueProtocol.ConsumerProtocolInterface
	prodToInput queueProtocol.ProducerProtocolInterface
}

// NewJourneyDispatcher Creates a new dispatcher
func NewJourneyDispatcher(input queueProtocol.ConsumerProtocolInterface, prodToInput queueProtocol.ProducerProtocolInterface, outputChannels []queueProtocol.ProducerProtocolInterface) *JourneyDispatcher {
	return &JourneyDispatcher{
		input:       input,
		channels:    outputChannels,
		prodToInput: prodToInput,
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
	if message.TypeMessage == dataStructures.EOFFlightRows {
		err := queueProtocol.HandleEOF(message, jd.input, jd.prodToInput, jd.channels)
		if err != nil {
			log.Errorf("JourneyDispatcher | Error handling EOF | %v", err)
		}
	} else if message.TypeMessage == dataStructures.FlightRows {
		jd.dispatchFlightRows(message)
	} else {
		log.Warnf("JourneyDispatcher | Warning Message | Unknown message received | Skipping it...")
	}
}

func (jd *JourneyDispatcher) dispatchFlightRows(message *dataStructures.Message) {
	for _, row := range message.DynMaps {
		startingAirport, err := row.GetAsBytes(utils.StartingAirport)
		if err != nil {
			log.Errorf("JourneyDispatcher | Error getting starting airport. Skipping row | %v", err)
			continue
		}
		destAirport, err := row.GetAsBytes(utils.DestinationAirport)
		if err != nil {
			log.Errorf("JourneyDispatcher | Error getting destination airport. Skipping row | %v", err)
			continue
		}
		var bytesToHash []byte
		bytesToHash = append(bytesToHash, startingAirport...)
		bytesToHash = append(bytesToHash, destAirport...)
		hasher := fnv.New32a()
		_, _ = hasher.Write(bytesToHash)
		hashRes := int(hasher.Sum32())
		log.Debugf("JourneyDispatcher | Deciding where to dispatch. hashRes is: %v; Len of channels is: %v", hashRes, len(jd.channels))
		resultIndex := hashRes % len(jd.channels)
		log.Debugf("JourneyDispatcher | Dispatching to Node #%v...", resultIndex)
		err = jd.channels[resultIndex].Send(&dataStructures.Message{TypeMessage: dataStructures.FlightRows, DynMaps: []*dataStructures.DynamicMap{row}, ClientId: message.ClientId})
		if err != nil {
			log.Errorf("JourneyDispatcher | Error sending message to queue #%v | %v | Skipping row...", resultIndex, err)
		}
	}
}
