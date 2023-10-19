package main

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	queueProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	log "github.com/sirupsen/logrus"
)

type JourneySink struct {
	journeysConsumer   queueProtocol.ConsumerProtocolInterface
	avgAndMaxProducer  queueProtocol.ProducerProtocolInterface
	totalJourneySavers uint
}

func NewJourneySink(journeysConsumer queueProtocol.ConsumerProtocolInterface, avgAndMaxProducer queueProtocol.ProducerProtocolInterface, totalJourneySavers uint) *JourneySink {
	return &JourneySink{journeysConsumer: journeysConsumer, avgAndMaxProducer: avgAndMaxProducer, totalJourneySavers: totalJourneySavers}
}

func (j *JourneySink) HandleJourneys() {
	for {
		for recvJourneys := uint(0); recvJourneys < j.totalJourneySavers; {
			msg, ok := j.journeysConsumer.Pop()
			if !ok {
				log.Infof("JourneySink | Consumer closed, exiting goroutine")
				return
			}
			if msg.TypeMessage == dataStructures.FlightRows {
				err := j.avgAndMaxProducer.Send(msg)
				if err != nil {
					log.Errorf("JourneySink | Error sending max and average to saver | %v", err)
					return
				}
			} else if msg.TypeMessage == dataStructures.EOFFlightRows {
				recvJourneys++
				log.Infof("JourneySink | Received EOF of one journey saver | Accumulated %v | Total: %v ", recvJourneys, j.totalJourneySavers)
			} else {
				log.Warnf("JourneySink | Received unexpected message type %v", msg.TypeMessage)
			}
		}
		log.Infof("JourneySink | Sending EOF to saver")
		msg := &dataStructures.Message{
			TypeMessage: dataStructures.EOFFlightRows,
		}
		err := j.avgAndMaxProducer.Send(msg)
		if err != nil {
			log.Errorf("JourneySink | Error sending EOF to saver | %v", err)
		}
	}
}
