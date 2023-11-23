package queuefactory

import (
	"github.com/brunograssano/Distribuidos-TP1/common/duplicates"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
)

type SimpleQueueFactory struct {
	qMiddleware middleware.QueueMiddlewareI
}

func NewSimpleQueueFactory(qMiddleware middleware.QueueMiddlewareI) QueueProtocolFactory {
	return &SimpleQueueFactory{qMiddleware: qMiddleware}
}

func (s *SimpleQueueFactory) CreateProducer(queueName string) queues.ProducerProtocolInterface {
	producer := s.qMiddleware.CreateProducer(queueName, true)
	return queues.NewProducerQueueProtocolHandler(producer)
}

func (s *SimpleQueueFactory) CreateConsumer(queueName string) queues.ConsumerProtocolInterface {
	consumer := s.qMiddleware.CreateConsumer(queueName, true)
	return queues.NewConsumerQueueProtocolHandler(consumer, duplicates.NewDuplicatesHandler())
}
