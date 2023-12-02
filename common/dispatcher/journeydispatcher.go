package dispatcher

import (
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	queueProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"hash/fnv"
)

// JourneyDispatcher Struct that dispatches journey messages
type JourneyDispatcher struct {
	id            int
	channels      []queueProtocol.ProducerProtocolInterface
	input         queueProtocol.ConsumerProtocolInterface
	prodToInput   queueProtocol.ProducerProtocolInterface
	checkpointer  *checkpointer.CheckpointerHandler
	totalEofNodes uint
	eofId         string
}

// NewJourneyDispatcher Creates a new dispatcher
func NewJourneyDispatcher(
	id uint,
	input queueProtocol.ConsumerProtocolInterface,
	prodToInput queueProtocol.ProducerProtocolInterface,
	outputChannels []queueProtocol.ProducerProtocolInterface,
	chkHandler *checkpointer.CheckpointerHandler,
	totalEofNodes uint,
	eofId string,
) *JourneyDispatcher {
	chkHandler.AddCheckpointable(input, int(id))
	return &JourneyDispatcher{
		id:            int(id),
		input:         input,
		channels:      outputChannels,
		prodToInput:   prodToInput,
		checkpointer:  chkHandler,
		totalEofNodes: totalEofNodes,
		eofId:         eofId,
	}
}

// DispatchLoop Listens to the input queue and dispatches the msg to a Journey Saver
func (jd *JourneyDispatcher) DispatchLoop() {
	log.Infof("JourneyDispatcher %v | Started Journey Dispatcher loop", jd.id)
	for {
		msg, ok := jd.input.Pop()
		if !ok {
			log.Infof("JourneyDispatcher %v | Input queue closed, stopping...", jd.id)
			return
		}
		log.Debugf("JourneyDispatcher %v | Received message, dispatching its rows to Journey Savers", jd.id)
		jd.dispatch(msg)
	}
}

// dispatch Executes the msg dispatch by calculating the sha256 hash
// on the starting airport and destination airport on each row of the message
func (jd *JourneyDispatcher) dispatch(message *dataStructures.Message) {
	if message.TypeMessage == dataStructures.EOFFlightRows {
		err := queueProtocol.HandleEOF(message, jd.prodToInput, jd.channels, jd.eofId, jd.totalEofNodes)
		if err != nil {
			log.Errorf("JourneyDispatcher | Error handling EOF | %v", err)
		}
	} else if message.TypeMessage == dataStructures.FlightRows {
		jd.dispatchFlightRows(message)
	} else {
		log.Warnf("JourneyDispatcher %v | Warning Message | Unknown message received | Skipping it...", jd.id)
	}
	err := jd.checkpointer.DoCheckpoint(jd.id)
	if err != nil {
		log.Errorf("JourneyDispatcher #%v | Error on checkpointing | %v", jd.id, err)
	}
}

func (jd *JourneyDispatcher) dispatchFlightRows(message *dataStructures.Message) {
	for idx, row := range message.DynMaps {
		startingAirport, err := row.GetAsBytes(utils.StartingAirport)
		if err != nil {
			log.Errorf("JourneyDispatcher %v | Error getting starting airport. Skipping row | %v", jd.id, err)
			continue
		}
		destAirport, err := row.GetAsBytes(utils.DestinationAirport)
		if err != nil {
			log.Errorf("JourneyDispatcher %v | Error getting destination airport. Skipping row | %v", jd.id, err)
			continue
		}
		var bytesToHash []byte
		bytesToHash = append(bytesToHash, startingAirport...)
		bytesToHash = append(bytesToHash, destAirport...)
		hasher := fnv.New32a()
		_, _ = hasher.Write(bytesToHash)
		hashRes := int(hasher.Sum32())
		log.Debugf("JourneyDispatcher %v | Deciding where to dispatch. hashRes is: %v; Len of channels is: %v", jd.id, hashRes, len(jd.channels))
		resultIndex := hashRes % len(jd.channels)
		log.Debugf("JourneyDispatcher %v | Dispatching to Node #%v...", jd.id, resultIndex)
		err = jd.channels[resultIndex].Send(
			dataStructures.NewMessageWithDataAndRowId(message, []*dataStructures.DynamicMap{row}, uint16(idx)),
		)
		if err != nil {
			log.Errorf("JourneyDispatcher %v | Error sending message to queue #%v | %v | Skipping row...", jd.id, resultIndex, err)
		}
	}
}
