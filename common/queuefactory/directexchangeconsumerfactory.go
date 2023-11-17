package queuefactory

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	log "github.com/sirupsen/logrus"
)

type DirectExchangeConsumerSimpleProdQueueFactory struct {
	qMiddleware middleware.QueueMiddlewareI
	routingKey  uint
	counter     uint
}

func NewDirectExchangeConsumerSimpleProdQueueFactory(qMiddleware middleware.QueueMiddlewareI, routingKey uint) QueueProtocolFactory {
	return &DirectExchangeConsumerSimpleProdQueueFactory{qMiddleware: qMiddleware, routingKey: routingKey, counter: uint(0)}
}

func (d *DirectExchangeConsumerSimpleProdQueueFactory) CreateProducer(queueName string) queues.ProducerProtocolInterface {
	p := d.qMiddleware.CreateProducer(queueName, true)
	return queues.NewProducerQueueProtocolHandler(p)
}

func (d *DirectExchangeConsumerSimpleProdQueueFactory) CreateConsumer(queueName string) queues.ConsumerProtocolInterface {
	routingKey := d.routingKey + d.counter
	consumerQueue := d.qMiddleware.CreateConsumer(fmt.Sprintf("%v-%v", queueName, routingKey), true)
	err := consumerQueue.BindTo(queueName, fmt.Sprintf("%v", routingKey), "direct")
	if err != nil {
		log.Fatalf("DirectExchangeConsumerSimpleProdQueueFactory | Error creating consumer: %v", err)
	}
	d.counter++
	return queues.NewConsumerQueueProtocolHandler(consumerQueue)
}
