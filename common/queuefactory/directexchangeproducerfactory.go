package queuefactory

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/duplicates"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
)

type DirectExchangeProducerSimpleConsQueueFactory struct {
	qMiddleware middleware.QueueMiddlewareI
	counter     uint
}

func NewDirectExchangeProducerSimpleConsQueueFactory(qMiddleware middleware.QueueMiddlewareI) QueueProtocolFactory {
	return &DirectExchangeProducerSimpleConsQueueFactory{qMiddleware: qMiddleware, counter: uint(0)}
}

func (d *DirectExchangeProducerSimpleConsQueueFactory) CreateProducer(exchangeName string) queues.ProducerProtocolInterface {
	e := d.qMiddleware.CreateExchangeProducer(exchangeName, fmt.Sprintf("%v", d.counter), "direct", true)
	d.counter++
	return queues.NewProducerQueueProtocolHandler(e)
}

func (d *DirectExchangeProducerSimpleConsQueueFactory) CreateConsumer(queueName string) queues.ConsumerProtocolInterface {
	consumer := d.qMiddleware.CreateConsumer(queueName, true)
	return queues.NewConsumerQueueProtocolHandler(consumer, duplicates.NewDuplicatesHandler())
}
