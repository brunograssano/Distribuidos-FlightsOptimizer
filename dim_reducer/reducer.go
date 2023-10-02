package main

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	log "github.com/sirupsen/logrus"
)

// Reducer Structure that reduces the dimensions of a row by removing columns
type Reducer struct {
	reducerId  int
	c          *ReducerConfig
	consumer   middleware.ConsumerInterface
	producer   middleware.ProducerInterface
	serializer *dataStructures.DynamicMapSerializer
}

// NewReducer Creates a new reducer
func NewReducer(reducerId int, qMiddleware *middleware.QueueMiddleware, c *ReducerConfig, serializer *dataStructures.DynamicMapSerializer) *Reducer {
	consumer := qMiddleware.CreateConsumer(c.InputQueueName, true)
	producer := qMiddleware.CreateProducer(c.OutputQueueName, true)
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
		cols := r.serializer.Deserialize(msg)
		reducedData, err := cols.ReduceToColumns(r.c.ColumnsToKeep)
		if err != nil {
			log.Errorf("action: reduce_columns | reducer_id: %v | result: fail | skipping row | error: %v", r.reducerId, err)
			continue
		}
		serialized := r.serializer.Serialize(reducedData)
		err = r.producer.Send(serialized)
		if err != nil {
			log.Errorf("Error trying to send message to output queue")
		}
	}
}
