package main

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	log "github.com/sirupsen/logrus"
)

// Reducer Structure that reduces the dimensions of a row by removing columns
type Reducer struct {
	reducerId  int
	c          *ReducerConfig
	consumer   protocol.ConsumerProtocolInterface
	producer   protocol.ProducerProtocolInterface
	prodToCons protocol.ProducerProtocolInterface
}

// NewReducer Creates a new reducer
func NewReducer(reducerId int, qMiddleware *middleware.QueueMiddleware, c *ReducerConfig) *Reducer {
	consumer := protocol.NewConsumerQueueProtocolHandler(qMiddleware.CreateConsumer(c.InputQueueName, true))
	producer := protocol.NewProducerQueueProtocolHandler(qMiddleware.CreateProducer(c.OutputQueueName, true))
	prodToCons := protocol.NewProducerQueueProtocolHandler(qMiddleware.CreateProducer(c.InputQueueName, true))
	return &Reducer{
		reducerId:  reducerId,
		c:          c,
		consumer:   consumer,
		producer:   producer,
		prodToCons: prodToCons,
	}
}

// ReduceDims Loop that waits for input from the queue, reduces the rows by removing columns
// and sends the result to the next step
func (r *Reducer) ReduceDims() {
	for {
		msg, ok := r.consumer.Pop()
		if !ok {
			log.Infof("Closing goroutine %v", r.reducerId)
			return
		}
		if msg.TypeMessage == dataStructures.EOFFlightRows {
			log.Infof("Received EOF. Now handling...")
			err := protocol.HandleEOF(msg, r.consumer, r.prodToCons, []protocol.ProducerProtocolInterface{r.producer})
			if err != nil {
				log.Errorf("Error handling EOF: %v", err)
			}
			return
		} else if msg.TypeMessage == dataStructures.FlightRows {
			log.Infof("Received flight rows. Now handling...")
			r.handleFlightRows(msg)
		} else {
			log.Warnf("Received unknown type message. Skipping it...")
		}
	}
}

func (r *Reducer) handleFlightRows(msg *dataStructures.Message) {
	var rows []*dataStructures.DynamicMap
	for _, row := range msg.DynMaps {
		reducedData, err := row.ReduceToColumns(r.c.ColumnsToKeep)
		if err != nil {
			log.Errorf("action: reduce_columns | reducer_id: %v | result: fail | skipping row | error: %v", r.reducerId, err)
			continue
		}
		rows = append(rows, reducedData)
	}
	msg = &dataStructures.Message{TypeMessage: dataStructures.FlightRows, DynMaps: rows}
	err := r.producer.Send(msg)
	if err != nil {
		log.Errorf("Error trying to send message to output queue")
	}
}
