package middleware

import (
	"fmt"
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

func CreateDeadLetterQueue(channel *amqp.Channel, idDeadLetterQueue string) amqp.Queue {
	queue, err := channel.QueueDeclare(
		idDeadLetterQueue,
		true,
		false,
		false,
		false,
		map[string]interface{}{"x-max-priority": 10},
	)
	FailOnError(err, fmt.Sprintf("Failed to declare dead letter queue %v", idDeadLetterQueue))
	err = channel.ExchangeDeclare(
		"dead_letter_exchange",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	FailOnError(err, fmt.Sprintf("Failed to declare exchange for dead letter queue %v", idDeadLetterQueue))
	err = channel.QueueBind(
		queue.Name,             // queue name
		idDeadLetterQueue,      // routing key
		"dead_letter_exchange", // exchange
		false,
		nil,
	)
	return queue
}
