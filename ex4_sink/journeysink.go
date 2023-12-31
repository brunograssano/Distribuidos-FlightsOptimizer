package main

import (
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	queueProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	log "github.com/sirupsen/logrus"
)

const sinkId = 0

type JourneySink struct {
	inputQueue                    queueProtocol.ConsumerProtocolInterface
	toSaver4Producer              queueProtocol.ProducerProtocolInterface
	totalJourneySavers            uint
	journeySaversReceivedByClient map[string]uint
	checkpointer                  *checkpointer.CheckpointerHandler
}

func NewJourneySink(
	inputQueue queueProtocol.ConsumerProtocolInterface,
	toSaver4Producer queueProtocol.ProducerProtocolInterface,
	totalJourneySavers uint,
	checkpointer *checkpointer.CheckpointerHandler,
) *JourneySink {
	js := &JourneySink{
		inputQueue:                    inputQueue,
		toSaver4Producer:              toSaver4Producer,
		totalJourneySavers:            totalJourneySavers,
		journeySaversReceivedByClient: make(map[string]uint),
		checkpointer:                  checkpointer,
	}
	checkpointer.AddCheckpointable(inputQueue, sinkId)
	checkpointer.AddCheckpointable(js, sinkId)
	return js
}

func (j *JourneySink) HandleJourneys() {
	for {
		msg, ok := j.inputQueue.Pop()
		if !ok {
			log.Infof("JourneySink | Consumer closed, exiting goroutine")
			return
		}
		if msg.TypeMessage == dataStructures.FlightRows {
			j.handleFlightRows(msg)
		} else if msg.TypeMessage == dataStructures.EOFFlightRows {
			j.handleEofMsg(msg)
		} else {
			log.Warnf("JourneySink | Received unexpected message type %v", msg.TypeMessage)
		}
		err := j.checkpointer.DoCheckpoint(sinkId)
		if err != nil {
			log.Errorf("JourneySink | Error on checkpointing | %v", err)
		}
	}
}

func (j *JourneySink) handleEofMsg(msg *dataStructures.Message) {
	_, exists := j.journeySaversReceivedByClient[msg.ClientId]
	if !exists {
		j.journeySaversReceivedByClient[msg.ClientId] = 0
	}
	j.journeySaversReceivedByClient[msg.ClientId]++
	log.Infof("JourneySink | Received EOF of one journey saver | Accumulated %v | Total: %v ", j.journeySaversReceivedByClient[msg.ClientId], j.totalJourneySavers)
	if j.journeySaversReceivedByClient[msg.ClientId] >= j.totalJourneySavers {
		j.sendEofToNext(msg)
	}
}

func (j *JourneySink) handleFlightRows(msg *dataStructures.Message) {
	err := j.toSaver4Producer.Send(msg)
	if err != nil {
		log.Errorf("JourneySink | Error sending max and average to saver | %v", err)
	}
}

func (j *JourneySink) sendEofToNext(oldMsg *dataStructures.Message) {
	log.Infof("JourneySink | Sending EOF to saver")
	err := j.toSaver4Producer.Send(oldMsg)
	if err != nil {
		log.Errorf("JourneySink | Error sending EOF to saver | %v", err)
	}
	delete(j.journeySaversReceivedByClient, oldMsg.ClientId)
}
