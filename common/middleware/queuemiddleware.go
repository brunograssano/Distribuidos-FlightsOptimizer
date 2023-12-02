package middleware

import (
	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
)

type QueueMiddlewareI interface {
	CreateConsumer(name string, durable bool, idDLQ string) ConsumerInterface
	CreateProducer(name string, durable bool) ProducerInterface
	CreateExchangeProducer(nameExchange string, routingKey string, typeExchange string, durable bool) ProducerInterface
	Close()
}

type QueueMiddleware struct {
	channels []*amqp.Channel
	conn     *amqp.Connection
}

func NewQueueMiddleware(address string) *QueueMiddleware {
	log.Infof("QueueMiddleware | Connecting to RabbitMQ")
	conn, err := amqp.Dial(address)
	FailOnError(err, "Failed to connect via Dial to RabbitMQ.")
	log.Infof("QueueMiddleware | Connected to RabbitMQ")
	return &QueueMiddleware{
		channels: []*amqp.Channel{},
		conn:     conn,
	}

}

func (qm *QueueMiddleware) createChannel() *amqp.Channel {
	ch, err := qm.conn.Channel()
	FailOnError(err, "Failed to create RabbitMQ Channel.")
	log.Infof("QueueMiddleware | Created RabbitMQ Channel")
	qm.channels = append(qm.channels, ch)
	return ch
}

func (qm *QueueMiddleware) CreateConsumer(name string, durable bool, idDLQ string) ConsumerInterface {
	return NewConsumer(qm.createChannel(), name, durable, idDLQ)
}

func (qm *QueueMiddleware) CreateProducer(name string, durable bool) ProducerInterface {
	return NewProducer(qm.createChannel(), name, durable)
}

func (qm *QueueMiddleware) CreateExchangeProducer(nameExchange string, routingKey string, typeExchange string, durable bool) ProducerInterface {
	return NewExchangeProducer(qm.createChannel(), nameExchange, routingKey, typeExchange, durable)
}

func (qm *QueueMiddleware) Close() {

	for i, channel := range qm.channels {
		err := channel.Close()
		if err != nil {
			log.Errorf("QueueMiddleware | Error closing QueueMiddleware Channel %v | %v", i, err)
		}
	}

	err := qm.conn.Close()
	if err != nil {
		log.Errorf("QueueMiddleware | Error closing QueueMiddleware Connection | %v", err)
	}
}
