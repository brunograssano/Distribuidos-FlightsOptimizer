package dispatcher

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"hash/fnv"
)

// JourneyDispatcher Struct that dispatches journey messages
type JourneyDispatcher struct {
	channels    []protocol.ProducerProtocolInterface
	input       protocol.ConsumerProtocolInterface
	prodToInput protocol.ProducerProtocolInterface
}

// NewJourneyDispatcher Creates a new dispatcher
func NewJourneyDispatcher(input protocol.ConsumerProtocolInterface, prodToInput protocol.ProducerProtocolInterface, outputChannels []protocol.ProducerProtocolInterface) *JourneyDispatcher {
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
		log.Infof("JourneyDispatcher | Received message, dispatching its rows to Journey Savers")
		jd.dispatch(msg)
	}
}

// dispatch Executes the msg dispatch by calculating the sha256 hash
// on the starting airport and destination airport on each row of the message
func (jd *JourneyDispatcher) dispatch(message *dataStructures.Message) {
	if message.TypeMessage == dataStructures.EOFFlightRows {
		err := protocol.HandleEOF(message, jd.input, jd.prodToInput, jd.channels)
		if err != nil {
			log.Errorf("Error handling EOF: %v", err)
		}
		/*for idx, channel := range jd.channels {
			log.Infof("Sending EOF to channel #%v", idx)
			err := channel.Send(message)
			if err != nil {
				log.Errorf("Error sending EOF to channel #%v: %v", idx, err)
			}
		}*/
	} else if message.TypeMessage == dataStructures.FlightRows {
		jd.dispatchFlightRows(message)
	} else {
		log.Warnf("Unknown message received. Skipping it...")
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
		hasher.Write(bytesToHash)
		hashRes := int(hasher.Sum32())
		log.Debugf("[DISPATCHER] Deciding where to dispatch. hashRes is: %v; Len of channels is: %v", hashRes, len(jd.channels))
		resultIndex := hashRes % len(jd.channels)
		log.Debugf("[DISPATCHER] Dispatching to Node #%v...", resultIndex)
		err = jd.channels[resultIndex].Send(&dataStructures.Message{TypeMessage: dataStructures.FlightRows, DynMaps: []*dataStructures.DynamicMap{row}})
		if err != nil {
			log.Errorf("Error sending message to queue #%v. Skipping row...", resultIndex)
		}
	}
}
