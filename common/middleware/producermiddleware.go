package middleware

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type Producer struct {
	ProducerInterface
	rabbitMQChannel *amqp.Channel
	queue           amqp.Queue
	name            string
}

const TimeoutSeconds = 5

func NewProducer(channel *amqp.Channel, name string, durable bool) *Producer {
	queue := CreateQueue(channel, name, durable)
	return &Producer{
		rabbitMQChannel: channel,
		queue:           queue,
		name:            name,
	}
}

func (queue *Producer) Send(data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), TimeoutSeconds*time.Second)
	defer cancel()
	err := queue.rabbitMQChannel.PublishWithContext(ctx,
		"",               // exchange
		queue.queue.Name, // routing key
		false,            // mandatory
		false,            // immediate
		amqp.Publishing{
			ContentType:  BinaryDataMime,
			Body:         data,
			DeliveryMode: amqp.Persistent,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to Publish content into queue: %v", err)
	}
	return nil
}

func (q *Producer) GetName() string {
	return q.name
}
