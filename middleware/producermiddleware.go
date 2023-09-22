package middleware

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type Producer struct {
	ProducerInterface
	rabbitMQChannel *amqp.Channel
	queue           amqp.Queue
}

const TIMEOUTSECONDS = 5

func NewProducer(channel *amqp.Channel, name string, durable bool) *Producer {
	queue := CreateQueue(channel, name, durable)
	return &Producer{
		rabbitMQChannel: channel,
		queue:           queue,
	}
}

func (queue *Producer) Send(data []byte) {
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
