package queuefactory

import (
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	log "github.com/sirupsen/logrus"
)

type UniqueExchangeQueueFactory struct {
	qMiddleware  middleware.QueueMiddlewareI
	exchangeName string
	routingKey   string
}

func NewUniqueExchangeQueueFactory(qMiddleware middleware.QueueMiddlewareI, exchangeName string, routingKey string) QueueProtocolFactory {
	return &UniqueExchangeQueueFactory{qMiddleware: qMiddleware, exchangeName: exchangeName, routingKey: routingKey}
}

func (s UniqueExchangeQueueFactory) CreateProducer(queueName string) queues.ProducerProtocolInterface {
	producer := s.qMiddleware.CreateProducer(queueName, true)
	return queues.NewProducerQueueProtocolHandler(producer)
}

func (s UniqueExchangeQueueFactory) CreateConsumer(queueName string) queues.ConsumerProtocolInterface {
	consumer := s.qMiddleware.CreateConsumer(queueName, true)
	err := consumer.BindTo(s.exchangeName, s.routingKey, "fanout")
	if err != nil {
		log.Fatalf("UniqueExchangeQueueFactory | Error trying to bind the consumer's queue to the exchange | %v", err)
	}
	return queues.NewConsumerQueueProtocolHandler(consumer)
}
