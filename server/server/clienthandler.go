package server

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
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
	outQueueAirports   middleware.ProducerInterface
	outQueueFlightRows middleware.ProducerInterface
	GetterAddresses    []string
}

func NewClientHandler(conn *communication.TCPSocket, outQueueAirports middleware.ProducerInterface, outQueueFlightRows middleware.ProducerInterface, GetterAddresses []string) *ClientHandler {
	sph := socketsProtocol.NewSocketProtocolHandler(conn)
	return &ClientHandler{
		conn:               sph,
		rowsSent:           0,
		outQueueAirports:   outQueueAirports,
		outQueueFlightRows: outQueueFlightRows,
		GetterAddresses:    GetterAddresses,
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
	for i := 0; i < len(ch.GetterAddresses); i++ {
		currSleep := 2
		socketGetter, err := communication.NewActiveTCPSocket(ch.GetterAddresses[i])
		if err != nil {
			log.Errorf("ClientHandler | Error trying to connect to getter for exercise %v | %v | Ending getter conn and returning error.", i+1, err)
			_ = socketGetter.Close()
			return err
		}
		getterSPH := socketsProtocol.NewSocketProtocolHandler(&socketGetter.TCPSocket)
		for {
			msg, err := getterSPH.Read()
			if err != nil {
				log.Errorf("ClientHandler | Error trying to read from getter #%v | %v | Ending loop...", i+1, err)
				_ = socketGetter.Close()
				return err
			}
			shouldBreak, shouldReconnect, err := ch.handleGetterMessage(msg, cliSPH, socketGetter)
			if shouldBreak {
				break
			}
			if shouldReconnect {
				newSPH, err := exponentialBackoffConnection(currSleep, socketGetter)
				if err != nil {
					return err
				}
				getterSPH = newSPH
			}
			if err != nil {
				log.Errorf("ClientHandler | Error handling getter message | %v", err)
			}
		}
		_ = socketGetter.Close()
	}
	return nil
}

func exponentialBackoffConnection(currSleep int, socketGetter *communication.ActiveTCPSocket) (*socketsProtocol.SocketProtocolHandler, error) {
	log.Infof("ClientHandler | Sleeping for %v seconds so that response may be ready later...", currSleep)
	time.Sleep(time.Duration(currSleep) * time.Second)
	if currSleep < maxSleep {
		currSleep = currSleep * 2
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
	err := ch.outQueueAirports.Send(serializer.SerializeMsg(message))
	if err != nil {
		log.Errorf("ClientHandler | Error sending airports to exchange | %v", err)
		return err
	}
	return nil
}

func (ch *ClientHandler) handleFlightRowMessage(message *dataStructures.Message) error {
	err := ch.outQueueFlightRows.Send(serializer.SerializeMsg(message))
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
	return ch.outQueueFlightRows.Send(serializer.SerializeMsg(message))
}

func (ch *ClientHandler) handleMessage(message *dataStructures.Message, cliSPH *socketsProtocol.SocketProtocolHandler) (bool, error) {
	log.Debugf("ClientHandler | Received Message | {type: %v, rowCount:%v}", message.TypeMessage, len(message.DynMaps))
	if message.TypeMessage == dataStructures.Airports || message.TypeMessage == dataStructures.EOFAirports {
		return false, ch.handleAirportMessage(message)
	}
	if message.TypeMessage == dataStructures.EOFFlightRows {
		return false, ch.handleEOFFlightRows(message)
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
