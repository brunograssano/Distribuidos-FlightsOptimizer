package middleware

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	channel *amqp.Channel
	conn    *amqp.Connection
}

func NewQueueMiddleware() *QueueMiddleware {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	FailOnError(err, "Failed to connect via Dial to RabbitMQ.")
	ch, err := conn.Channel()
	FailOnError(err, "Failed to create RabbitMQ Channel.")
	return &QueueMiddleware{
		channel: ch,
		conn:    conn,
	}
}

func (qm *QueueMiddleware) CreateConsumer(name string, durable bool) ConsumerInterface {
	return NewConsumer(qm.channel, name, durable)
}

func (qm *QueueMiddleware) CreateProducer(name string, durable bool) ProducerInterface {
	return NewProducer(qm.channel, name, durable)
}

func (qm *QueueMiddleware) Close() {
	err := qm.channel.Close()
	FailOnError(err, "Error closing QueueMiddleware Channel")
	err = qm.conn.Close()
	FailOnError(err, "Error closing QueueMiddleware Connection")
}
