package main

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	"github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	log "github.com/sirupsen/logrus"
	"time"
)

const maxSleep = 32

type ClientHandler struct {
	rowsSent           uint
	conn               *protocol.SocketProtocolHandler
	outQueueAirports   middleware.ProducerInterface
	outQueueFlightRows middleware.ProducerInterface
	GetterAddresses    []string
	serializer         *data_structures.Serializer
}

func NewClientHandler(conn *communication.TCPSocket, outQueueAirports middleware.ProducerInterface, outQueueFlightRows middleware.ProducerInterface, GetterAddresses []string) *ClientHandler {
	sph := protocol.NewSocketProtocolHandler(conn)
	serializer := data_structures.NewSerializer()
	return &ClientHandler{
		conn:               sph,
		rowsSent:           0,
		outQueueAirports:   outQueueAirports,
		outQueueFlightRows: outQueueFlightRows,
		GetterAddresses:    GetterAddresses,
		serializer:         serializer,
	}
}

func (ch *ClientHandler) handleGetterMessage(
	msg *data_structures.Message,
	cliSPH *protocol.SocketProtocolHandler,
	socketGetter *communication.ActiveTCPSocket,
) (bool, bool, error) {
	// Sends the results to the client.
	// If the getter finishes it notifies the client the end of an exercise results
	if msg.TypeMessage == data_structures.FlightRows {
		err := cliSPH.Write(msg)
		if err != nil {
			log.Errorf("Error trying to send to client. Ending loop...")
			_ = socketGetter.Close()
			return false, false, err
		}
	} else if msg.TypeMessage == data_structures.EOFGetter {
		_ = socketGetter.Close()
		return true, false, nil
	} else if msg.TypeMessage == data_structures.Later {
		return false, true, nil
	} else {
		log.Warnf("Received unexpected message. Ignoring it...")
	}
	return false, false, nil
}

func (ch *ClientHandler) handleGetResults(cliSPH *protocol.SocketProtocolHandler) error {
	for i := 0; i < len(ch.GetterAddresses); i++ {
		currSleep := 2
		socketGetter, err := communication.NewActiveTCPSocket(ch.GetterAddresses[i])
		if err != nil {
			log.Errorf("Error trying to connect to getter for exercise %v. Ending getter conn and returning error.", i+1)
			_ = socketGetter.Close()
			return err
		}
		getterSPH := protocol.NewSocketProtocolHandler(&socketGetter.TCPSocket)
		for {
			msg, err := getterSPH.Read()
			if err != nil {
				log.Errorf("Error trying to read from getter #%v: %v. Ending loop...", i+1, err)
				_ = socketGetter.Close()
				return err
			}
			shouldBreak, shouldReconnect, err := ch.handleGetterMessage(msg, cliSPH, socketGetter)
			if shouldBreak {
				break
			}
			if shouldReconnect {
				log.Infof("Sleeping for %v seconds so that response may be ready later...", currSleep)
				time.Sleep(time.Duration(currSleep) * time.Second)
				if currSleep < maxSleep {
					currSleep = currSleep * 2
				}
				err := socketGetter.Reconnect()
				if err != nil {
					log.Errorf("Error trying to reconnect to getter... Ending...")
					_ = socketGetter.Close()
					return err
				}
				getterSPH = protocol.NewSocketProtocolHandler(socketGetter)
			}
			if err != nil {
				log.Errorf("Error handling getter message: %v", err)
			}
		}
		_ = socketGetter.Close()
	}
	return nil
}

func (ch *ClientHandler) handleAirportMessage(message *data_structures.Message) error {
	log.Infof("Sending airports to exchange...")
	err := ch.outQueueAirports.Send(ch.serializer.SerializeMsg(message))
	if err != nil {
		log.Infof("Error sending airports to exchange: %v", err)
		return err
	}
	return nil
}

func (ch *ClientHandler) handleFlightRowMessage(message *data_structures.Message) error {
	err := ch.outQueueFlightRows.Send(ch.serializer.SerializeMsg(message))
	if err != nil {
		return err
	}

	return nil
}

func (ch *ClientHandler) handleEOFFlightRows(message *data_structures.Message) error {
	dynMap := data_structures.NewDynamicMap(make(map[string][]byte))
	dynMap.AddColumn("prevSent", ch.serializer.SerializeUint(uint32(ch.rowsSent)))
	dynMap.AddColumn("localSent", ch.serializer.SerializeUint(uint32(0)))
	dynMap.AddColumn("localReceived", ch.serializer.SerializeUint(uint32(0)))
	message.DynMaps = append(message.DynMaps, dynMap)

	return ch.outQueueFlightRows.Send(ch.serializer.SerializeMsg(message))
}

func (ch *ClientHandler) handleMessage(message *data_structures.Message, cliSPH *protocol.SocketProtocolHandler) error {
	log.Infof("Received Message: {type: %v, rowCount:%v}", message.TypeMessage, len(message.DynMaps))
	if message.TypeMessage == data_structures.Airports || message.TypeMessage == data_structures.EOFAirports {
		return ch.handleAirportMessage(message)
	}
	if message.TypeMessage == data_structures.EOFFlightRows {
		return ch.handleEOFFlightRows(message)
	}
	if message.TypeMessage == data_structures.FlightRows {
		return ch.handleFlightRowMessage(message)
	}
	if message.TypeMessage == data_structures.GetResults {
		return ch.handleGetResults(cliSPH)
	}
	return fmt.Errorf("unrecognized message type: %v", message.TypeMessage)
}

func (ch *ClientHandler) StartClientLoop() {

	defer ch.conn.Close()
	for {
		message, err := ch.conn.Read()
		if err != nil {
			log.Errorf("Error trying to receive message: %v. Ending client handle & Closing socket...", err)
			return
		}
		log.Debugf("Handling message from client %v", message)
		err = ch.handleMessage(message, ch.conn)
		if err != nil {
			log.Errorf("%v", err)
			return
		}
	}
}
