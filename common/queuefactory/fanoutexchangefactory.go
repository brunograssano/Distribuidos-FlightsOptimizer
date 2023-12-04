package queuefactory

import (
	"github.com/brunograssano/Distribuidos-TP1/common/duplicates"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	log "github.com/sirupsen/logrus"
)

type FanoutExchangeQueueFactory struct {
	qMiddleware  middleware.QueueMiddlewareI
	exchangeName string
	routingKey   string
}

func NewFanoutExchangeQueueFactory(qMiddleware middleware.QueueMiddlewareI, exchangeName string, routingKey string) QueueProtocolFactory {
	return &FanoutExchangeQueueFactory{qMiddleware: qMiddleware, exchangeName: exchangeName, routingKey: routingKey}
}

func (s *FanoutExchangeQueueFactory) CreateProducer(_ string) queues.ProducerProtocolInterface {
	producer := s.qMiddleware.CreateExchangeProducer(s.exchangeName, s.routingKey, "fanout", true)
	return queues.NewProducerQueueProtocolHandler(producer)
}

func (s *FanoutExchangeQueueFactory) CreateConsumer(queueName string) queues.ConsumerProtocolInterface {
	consumer := s.qMiddleware.CreateConsumer(queueName, true)
	err := consumer.BindTo(s.exchangeName, s.routingKey, "fanout")
	if err != nil {
		log.Fatalf("FanoutExchangeQueueFactory | Error trying to bind the consumer's queue to the exchange | %v", err)
	}
	return queues.NewConsumerQueueProtocolHandler(consumer, duplicates.NewDuplicatesHandler(consumer.GetName()))
}
