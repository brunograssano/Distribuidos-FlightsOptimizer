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
	serializer *dataStructures.Serializer
}

// NewReducer Creates a new reducer
func NewReducer(reducerId int, qMiddleware *middleware.QueueMiddleware, c *ReducerConfig, serializer *dataStructures.Serializer) *Reducer {
	consumer := protocol.NewConsumerQueueProtocolHandler(qMiddleware.CreateConsumer(c.InputQueueName, true))
	producer := protocol.NewProducerQueueProtocolHandler(qMiddleware.CreateProducer(c.OutputQueueName, true))
	return &Reducer{
		reducerId:  reducerId,
		c:          c,
		consumer:   consumer,
		producer:   producer,
		serializer: serializer,
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
}
