package middleware

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

func CreateQueue(channel *amqp.Channel, name string, durable bool) amqp.Queue {
	queue, err := channel.QueueDeclare(
		name,
		durable, // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	FailOnError(err, "Failed to declare RabbitMQ Queue.")
	return queue
}
