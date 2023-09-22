package middleware

import amqp "github.com/rabbitmq/amqp091-go"

type Consumer struct {
	ConsumerInterface
	rabbitMQChannel *amqp.Channel
	messageChannel  <-chan amqp.Delivery
	queue           amqp.Queue
}

func NewConsumer(channel *amqp.Channel, name string, durable bool) *Consumer {
	queue := CreateQueue(channel, name, durable)
	messages, err := channel.Consume(
		queue.Name, // queue
		"",         // consumer
		true,       // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	FailOnError(err, "Failed to consume and create bytes channel in the RabbitMQ Queue.")
	return &Consumer{
		rabbitMQChannel: channel,
		queue:           queue,
		messageChannel:  messages,
	}
}

func (queue *Consumer) Pop() []byte {
	msg := <-queue.messageChannel
	return msg.Body
}
