package middleware

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

const TIMEOUTSECONDS = 5

type RabbitMQQueue struct {
	QueueInterface
	rabbitMQChannel *amqp.Channel
	messageChannel  <-chan amqp.Delivery
	queue           amqp.Queue
}

func NewRabbitMqImplementation(channel *amqp.Channel, name string, durable bool) *RabbitMQQueue {
	queue, err := channel.QueueDeclare(
		name,
		durable, // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	FailOnError(err, "Failed to declare RabbitMQ Queue.")

	return &RabbitMQQueue{
		rabbitMQChannel: channel,
		queue:           queue,
	}
}

func (queue *RabbitMQQueue) Send(data []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUTSECONDS*time.Second)
	defer cancel()
	err := queue.rabbitMQChannel.PublishWithContext(ctx,
		"",               // exchange
		queue.queue.Name, // routing key
		false,            // mandatory
		false,            // immediate
		amqp.Publishing{
			ContentType:  "application/octet-stream",
			Body:         data,
			DeliveryMode: amqp.Persistent,
		},
	)
	FailOnError(err, "Failed to Publish content into queue.")
}

func (queue *RabbitMQQueue) BecomeConsumer() {
	messages, err := queue.rabbitMQChannel.Consume(
		queue.queue.Name, // queue
		"",               // consumer
		true,             // auto-ack
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)
	FailOnError(err, "Failed to consume and create bytes channel in the RabbitMQ Queue.")
	queue.messageChannel = messages
}

func (queue *RabbitMQQueue) Pop() []byte {
	msg := <-queue.messageChannel
	return msg.Body
}
