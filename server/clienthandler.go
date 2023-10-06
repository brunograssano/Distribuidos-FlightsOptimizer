package main

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	"github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	log "github.com/sirupsen/logrus"
)

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

func (ch *ClientHandler) handleGetResults(cliSPH *protocol.SocketProtocolHandler) error {
	for i := 0; i < len(ch.GetterAddresses); i++ {
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
				log.Errorf("Error trying to read from getter #%v. Ending loop...", i+1)
				_ = socketGetter.Close()
				return err
			}

			// Sends the results to the client.
			// If the getter finishes it notifies the client the end of an exercise results
			err = cliSPH.Write(msg)
			if err != nil {
				log.Errorf("Error trying to send to client. Ending loop...")
				_ = socketGetter.Close()
				return err
			}

			// If the getter finishes we stop the innermost loop
			if msg.TypeMessage == data_structures.EOFGetter {
				_ = socketGetter.Close()
				break
			}
		}
		_ = socketGetter.Close()
	}
	return nil
}

func (ch *ClientHandler) handleAirportMessage(message *data_structures.Message) error {
	err := ch.outQueueAirports.Send(ch.serializer.SerializeMsg(message))
	if err != nil {
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
	dynMap.AddColumn("totalSent", ch.serializer.SerializeUint(uint32(ch.rowsSent)))
	dynMap.AddColumn("localSent", ch.serializer.SerializeUint(uint32(0)))
	dynMap.AddColumn("localReceived", ch.serializer.SerializeUint(uint32(0)))
	message.DynMaps = append(message.DynMaps, dynMap)

	return ch.outQueueFlightRows.Send(ch.serializer.SerializeMsg(message))
}

func (ch *ClientHandler) handleMessage(message *data_structures.Message, cliSPH *protocol.SocketProtocolHandler) error {
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
