package main

import (
	dataStrutures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	log "github.com/sirupsen/logrus"
)

type Reducer struct {
	reducerId int
	c         *ReducerConfig
	consumer  middleware.ConsumerInterface
	producer  middleware.ProducerInterface
}

func NewReducer(reducerId int, qMiddleware *middleware.QueueMiddleware, c *ReducerConfig) *Reducer {
	consumer := qMiddleware.CreateConsumer(c.InputQueueName, true)
	producer := qMiddleware.CreateProducer(c.OutputQueueName, true)
	return &Reducer{
		reducerId: reducerId,
		c:         c,
		consumer:  consumer,
		producer:  producer,
	}
}

func (r *Reducer) ReduceDims() {
	for {
		msg, ok := r.consumer.Pop()
		if !ok {
			log.Infof("Closing goroutine %v", r.reducerId)
			return
		}
		cols := dataStrutures.Deserialize(msg)
		reducedData, err := cols.ReduceToColumns(r.c.ColumnsToKeep)
		if err != nil {
			log.Errorf("action: reduce_columns | reducer_id: %v | result: fail | skipping row | error: %v", r.reducerId, err)
			continue
		}
		serialized := dataStrutures.Serialize(reducedData)
		r.producer.Send(serialized)
	}
}
