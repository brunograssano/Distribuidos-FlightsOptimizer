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

func (qm *QueueMiddleware) CreateQueue(name string, durable bool) QueueInterface {
	return NewRabbitMqImplementation(qm.channel, name, durable)
}

func (qm *QueueMiddleware) Close() {
	qm.channel.Close()
	qm.conn.Close()
}
