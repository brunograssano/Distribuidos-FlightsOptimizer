package server

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/getters"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	socketsProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/sockets"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"time"
)

const maxSleep = 32
const initialExpBackoffSleep = 2

type ClientHandler struct {
	rowsSent           uint
	conn               *socketsProtocol.SocketProtocolHandler
	outQueueAirports   queues.ProducerProtocolInterface
	outQueueFlightRows queues.ProducerProtocolInterface
	GetterAddresses    map[uint8][]string
	clientId           string
}

func NewClientHandler(conn *communication.TCPSocket, outQueueAirports middleware.ProducerInterface, outQueueFlightRows middleware.ProducerInterface, GetterAddresses map[uint8][]string) *ClientHandler {
	sph := socketsProtocol.NewSocketProtocolHandler(conn)
	return &ClientHandler{
		conn:               sph,
		rowsSent:           0,
		outQueueAirports:   queues.NewProducerQueueProtocolHandler(outQueueAirports),
		outQueueFlightRows: queues.NewProducerQueueProtocolHandler(outQueueFlightRows),
		GetterAddresses:    GetterAddresses,
		clientId:           "",
	}
}

func (ch *ClientHandler) handleGetResults(cliSPH *socketsProtocol.SocketProtocolHandler, row int, exercise int) error {
	addresses, exists := ch.GetterAddresses[uint8(exercise)]
	currSleep := initialExpBackoffSleep
	if !exists {
		log.Errorf("ClientHandler | Exercise %v does not exist | Not handling...", exercise)
		return nil
	}
	getterSPH, getterActiveSocket := ch.initializeCommunicationWithGetter(addresses, exercise, row)
	currRow := row
	for {
		msg, err := getterSPH.Read()
		if err != nil {
			log.Errorf("ClientHandler | Error trying to read from getter #%v | %v", exercise, err)
			getterSPH.Close()
			getterSPH, getterActiveSocket = ch.initializeCommunicationWithGetter(addresses, exercise, currRow)
			continue
		}
		if msg.TypeMessage == dataStructures.Later {
			getterSPH, err = ch.handleLaterMessageFromGetter(getterSPH, err, &currSleep, getterActiveSocket, addresses, exercise, currRow)
			continue
		}
		err = cliSPH.Write(msg)
		if err != nil {
			log.Errorf("ClientHandler | Error retransmiting message to client from getter of ex %v| %v", exercise, err)
		}
		currRow += len(msg.DynMaps)
		if msg.TypeMessage == dataStructures.EOFGetter {
			break
		}
	}
	getterSPH.Close()
	return nil
}

func (ch *ClientHandler) handleLaterMessageFromGetter(getterSPH *socketsProtocol.SocketProtocolHandler, err error, currSleep *int, getterActiveSocket *communication.ActiveTCPSocket, addresses []string, exercise int, currRow int) (*socketsProtocol.SocketProtocolHandler, error) {
	getterSPH.Close()
	exponentialBackoffConnection(currSleep)
	getterSPH, getterActiveSocket = ch.initializeCommunicationWithGetter(addresses, exercise, currRow)
	return getterSPH, err
}

func (ch *ClientHandler) initializeCommunicationWithGetter(addresses []string, exercise int, row int) (*socketsProtocol.SocketProtocolHandler, *communication.ActiveTCPSocket) {
	currSleep := initialExpBackoffSleep
	var socketGetter *communication.ActiveTCPSocket
	for {
		socketGetter = ch.connectToGetter(addresses, uint8(exercise))
		if socketGetter != nil {
			break
		}
		log.Warnf("ClientHandler | Could not connect to getters for exercise %v | Now sleeping for %v", exercise, currSleep)
		exponentialBackoffConnection(&currSleep)
	}
	getterSPH := socketsProtocol.NewSocketProtocolHandler(&socketGetter.TCPSocket)
	initialMessage := getters.GetExerciseMessageWithRow(ch.clientId, exercise, row)
	err := getterSPH.Write(initialMessage)
	if err != nil {
		log.Errorf("ClientHandler | Error trying to write to getter #%v | %v ", exercise, err)
	}
	return getterSPH, socketGetter
}

func (ch *ClientHandler) connectToGetter(addresses []string, ex uint8) *communication.ActiveTCPSocket {
	for _, address := range addresses {
		socketGetter, _ := communication.NewActiveTCPSocket(address)
		if socketGetter != nil {
			return socketGetter
		}
	}
	return nil
}

func exponentialBackoffConnection(currSleep *int) {
	log.Infof("ClientHandler | Sleeping for %v seconds so that response may be ready later...", *currSleep)
	time.Sleep(time.Duration(*currSleep) * time.Second)
	if *currSleep < maxSleep {
		*currSleep = (*currSleep) * 2
		if *currSleep > maxSleep {
			*currSleep = maxSleep
		}
	}
	*currSleep = initialExpBackoffSleep
}

func (ch *ClientHandler) handleAirportMessage(message *dataStructures.Message) error {
	log.Debugf("ClientHandler | Sending airports to exchange...")
	err := ch.outQueueAirports.Send(message)
	if err != nil {
		log.Errorf("ClientHandler | Error sending airports to exchange | %v", err)
		return err
	}
	return nil
}

func (ch *ClientHandler) handleFlightRowMessage(message *dataStructures.Message) error {
	err := ch.outQueueFlightRows.Send(message)
	ch.rowsSent += uint(len(message.DynMaps))
	if err != nil {
		return err
	}

	return nil
}

func (ch *ClientHandler) handleEOFFlightRows(message *dataStructures.Message) error {
	dynMap := dataStructures.NewDynamicMap(make(map[string][]byte))
	dynMap.AddColumn(utils.NodesVisited, serializer.SerializeString(""))
	message.DynMaps = append(message.DynMaps, dynMap)
	log.Infof("ClientHandler | Sending EOF | Batches sent: %v", ch.rowsSent)
	return ch.outQueueFlightRows.Send(message)
}

func (ch *ClientHandler) handleMessage(message *dataStructures.Message, cliSPH *socketsProtocol.SocketProtocolHandler) error {
	log.Debugf("ClientHandler | Received Message | {type: %v, rowCount:%v}", message.TypeMessage, len(message.DynMaps))
	ch.clientId = message.ClientId
	if message.TypeMessage == dataStructures.Airports || message.TypeMessage == dataStructures.EOFAirports {
		if message.TypeMessage == dataStructures.EOFAirports {
			log.Infof("ClientHandler | Got EOF Airports | ClientId: %v", message.ClientId)
		}
		err := ch.handleAirportMessage(message)
		if message.TypeMessage == dataStructures.EOFAirports {
			log.Infof("ClientHandler | Sending ACK for EOF Airports | ClientId: %v", message.ClientId)
			err = cliSPH.Write(dataStructures.NewCompleteMessage(dataStructures.EofAck, []*dataStructures.DynamicMap{}, message.ClientId, message.MessageId))
		}
		return err
	}
	if message.TypeMessage == dataStructures.EOFFlightRows {
		log.Infof("ClientHandler | Got EOF FlightRows | ClientId: %v", message.ClientId)
		err := ch.handleEOFFlightRows(message)
		if err == nil {
			log.Infof("ClientHandler | Sending ACK for EOF FlightRows | ClientId: %v", message.ClientId)
			err = cliSPH.Write(dataStructures.NewCompleteMessage(dataStructures.EofAck, []*dataStructures.DynamicMap{}, message.ClientId, message.MessageId))
		}
		return err
	}
	if message.TypeMessage == dataStructures.FlightRows {
		return ch.handleFlightRowMessage(message)
	}
	if message.TypeMessage == dataStructures.GetResults {
		ex, err := message.DynMaps[0].GetAsInt(utils.Exercise)
		if err != nil {
			log.Errorf("ClientHandler | Error getting exercise as int | %v", err)
		}
		row, err := message.DynMaps[0].GetAsInt(utils.NumberOfRow)
		if err != nil {
			log.Errorf("ClientHandler | Error getting row as int | %v", err)
		}
		return ch.handleGetResults(cliSPH, row, ex)
	}
	return fmt.Errorf("unrecognized message type: %v", message.TypeMessage)
}

func (ch *ClientHandler) StartClientLoop() {
	defer ch.conn.Close()
	for {
		message, err := ch.conn.Read()
		if err != nil {
			log.Errorf("ClientHandler | Error trying to receive message | %v | Ending client handle & Closing socket...", err)
			return
		}
		log.Debugf("ClientHandler | Handling message from client | %v", message)
		err = ch.handleMessage(message, ch.conn)
		if err != nil {
			log.Errorf("ClientHandler | Error handling message | %v", err)
			return
		}
	}
}
