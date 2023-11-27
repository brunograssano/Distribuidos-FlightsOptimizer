package server

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	socketsProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/sockets"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"time"
)

const maxSleep = 32

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

func (ch *ClientHandler) handleGetterMessage(
	msg *dataStructures.Message,
	cliSPH *socketsProtocol.SocketProtocolHandler,
	socketGetter *communication.ActiveTCPSocket,
) (bool, bool, error) {
	// Sends the results to the client.
	// If the getter finishes it notifies the client the end of an exercise results
	if msg.TypeMessage == dataStructures.FlightRows {
		err := cliSPH.Write(msg)
		if err != nil {
			log.Errorf("ClientHandler | Error trying to send to client | %v | Ending loop...", err)
			_ = socketGetter.Close()
			return false, false, err
		}
	} else if msg.TypeMessage == dataStructures.EOFGetter {
		err := cliSPH.Write(msg)
		if err != nil {
			log.Errorf("ClientHandler | Error trying to send EOFGetter to client | %v | Ending loop...", err)
			_ = socketGetter.Close()
			return true, false, err
		}
		_ = socketGetter.Close()
		return true, false, nil
	} else if msg.TypeMessage == dataStructures.Later {
		return false, true, nil
	} else {
		log.Warnf("ClientHandler | Warning Message | Received unexpected message | Ignoring it...")
	}
	return false, false, nil
}

func (ch *ClientHandler) handleGetResults(cliSPH *socketsProtocol.SocketProtocolHandler) error {
	for ex, addresses := range ch.GetterAddresses {
		currSleep := 2
		socketGetter := ch.connectToGetter(addresses, ex)

		getterSPH := socketsProtocol.NewSocketProtocolHandler(&socketGetter.TCPSocket)
		initialMessage := dataStructures.NewGetResultsMessage(ch.clientId)
		err := getterSPH.Write(initialMessage)
		if err != nil {
			log.Errorf("ClientHandler | Error trying to write to getter #%v | %v | Ending loop...", ex, err)
		}
		for {

			msg, err := getterSPH.Read()
			if err != nil {
				log.Errorf("ClientHandler | Error trying to read from getter #%v | %v | Ending loop...", ex, err)
				_ = socketGetter.Close()
				return err
			}
			shouldBreak, shouldReconnect, err := ch.handleGetterMessage(msg, cliSPH, socketGetter)
			if shouldBreak {
				break
			}
			if shouldReconnect {
				//TODO: Ver que hacemos con esto. Reconectar a otro server? Ver hasta donde llego la respuesta?
				newSPH, err := exponentialBackoffConnection(&currSleep, socketGetter)
				if err != nil {
					return err
				}
				getterSPH = newSPH
				err = getterSPH.Write(initialMessage)
				if err != nil {
					log.Errorf("ClientHandler | Error trying to write to getter #%v | %v | Ending loop...", ex, err)
				}
			}
			if err != nil {
				log.Errorf("ClientHandler | Error handling getter message | %v", err)
			}
		}
		_ = socketGetter.Close()
	}
	return nil
}

func (ch *ClientHandler) connectToGetter(addresses []string, ex uint8) *communication.ActiveTCPSocket {
	// TODO si fallo con todos exponential backoff
	for _, address := range addresses {
		socketGetter, err := communication.NewActiveTCPSocket(address)
		if err != nil {
			log.Errorf("ClientHandler | Error trying to connect to getter for exercise %v | %v | Ending getter conn and returning error. | Address: %v", ex+1, err, address)
		}
		if socketGetter != nil {
			return socketGetter
		}
	}
	return nil
}

func exponentialBackoffConnection(currSleep *int, socketGetter *communication.ActiveTCPSocket) (*socketsProtocol.SocketProtocolHandler, error) {
	log.Infof("ClientHandler | Sleeping for %v seconds so that response may be ready later...", currSleep)
	time.Sleep(time.Duration(*currSleep) * time.Second)
	if *currSleep < maxSleep {
		*currSleep = (*currSleep) * 2
	}
	err := socketGetter.Reconnect()
	if err != nil {
		log.Errorf("ClientHandler | Error trying to reconnect to getter... Ending...")
		_ = socketGetter.Close()
		return nil, err
	}
	return socketsProtocol.NewSocketProtocolHandler(socketGetter), nil
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
	dynMap.AddColumn(utils.PrevSent, serializer.SerializeUint(uint32(ch.rowsSent)))
	dynMap.AddColumn(utils.LocalSent, serializer.SerializeUint(uint32(0)))
	dynMap.AddColumn(utils.LocalReceived, serializer.SerializeUint(uint32(0)))
	message.DynMaps = append(message.DynMaps, dynMap)
	log.Infof("ClientHandler | Sending EOF | Batches sent: %v", ch.rowsSent)
	return ch.outQueueFlightRows.Send(message)
}

func (ch *ClientHandler) handleMessage(message *dataStructures.Message, cliSPH *socketsProtocol.SocketProtocolHandler) (bool, error) {
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
		return false, err
	}
	if message.TypeMessage == dataStructures.EOFFlightRows {
		log.Infof("ClientHandler | Got EOF FlightRows | ClientId: %v", message.ClientId)
		err := ch.handleEOFFlightRows(message)
		if err == nil {
			log.Infof("ClientHandler | Sending ACK for EOF FlightRows | ClientId: %v", message.ClientId)
			err = cliSPH.Write(dataStructures.NewCompleteMessage(dataStructures.EofAck, []*dataStructures.DynamicMap{}, message.ClientId, message.MessageId))
		}
		return false, err
	}
	if message.TypeMessage == dataStructures.FlightRows {
		return false, ch.handleFlightRowMessage(message)
	}
	if message.TypeMessage == dataStructures.GetResults {
		return true, ch.handleGetResults(cliSPH)
	}
	return false, fmt.Errorf("unrecognized message type: %v", message.TypeMessage)
}

func (ch *ClientHandler) StartClientLoop() {
	defer ch.conn.Close()
	for endConn := false; !endConn; {
		message, err := ch.conn.Read()
		if err != nil {
			log.Errorf("ClientHandler | Error trying to receive message | %v | Ending client handle & Closing socket...", err)
			return
		}
		log.Debugf("ClientHandler | Handling message from client | %v", message)
		endConn, err = ch.handleMessage(message, ch.conn)
		if err != nil {
			log.Errorf("ClientHandler | Error handling message | %v", err)
			return
		}
	}
}
