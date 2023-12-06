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
	err = channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	FailOnError(err, "Failed to declare Prefetch count to 1.")
	return queue
}
