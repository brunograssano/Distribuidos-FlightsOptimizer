package queuefactory

import (
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	log "github.com/sirupsen/logrus"
)

type TopicFactory struct {
	qMiddleware  middleware.QueueMiddlewareI
	routingKeys  []string
	exchangeName string
}

func NewTopicFactory(qMiddleware middleware.QueueMiddlewareI, routingKeys []string, exchangeName string) QueueProtocolFactory {
	return &TopicFactory{qMiddleware, routingKeys, exchangeName}
}

func (t *TopicFactory) CreateProducer(routingKey string) queues.ProducerProtocolInterface {
	producer := t.qMiddleware.CreateExchangeProducer(t.exchangeName, routingKey, "topic", true)
	return queues.NewProducerQueueProtocolHandler(producer)
}

func (t *TopicFactory) CreateConsumer(queueName string) queues.ConsumerProtocolInterface {
	consumer := t.qMiddleware.CreateConsumer(queueName, true)
	for _, rk := range t.routingKeys {
		err := consumer.BindTo(t.exchangeName, rk, "topic")
		if err != nil {
			log.Fatalf("TopicFactory | Error trying to bind the consumer's queue to the exchange | %v", err)
		}
	}

	return queues.NewConsumerQueueProtocolHandler(consumer)
}
