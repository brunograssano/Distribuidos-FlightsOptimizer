package middleware

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
	"time"
)

const BinaryDataMime = "application/octet-stream"

type ExchangeProducer struct {
	ProducerInterface
	rabbitMQChannel *amqp.Channel
	name            string
	routingKey      string
}

func NewExchangeProducer(channel *amqp.Channel, nameEx string, routingKey string, typeEx string, durable bool) *ExchangeProducer {
	err := channel.ExchangeDeclare(
		nameEx,
		typeEx,
		durable,
		false,
		false,
		false,
		nil,
	)
	FailOnError(err, fmt.Sprintf("Failed to declare the Exchange %v in RabbitMQ", nameEx))
	log.Infof("ExchangeProducer | Created new exchange %v in RabbitMQ", nameEx)
	return &ExchangeProducer{
		rabbitMQChannel: channel,
		name:            nameEx,
		routingKey:      routingKey,
	}
}

func (exProd *ExchangeProducer) Send(data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), TimeoutSeconds*time.Second)
	defer cancel()
	err := exProd.rabbitMQChannel.PublishWithContext(ctx,
		exProd.name,       // exchange
		exProd.routingKey, // routing key
		false,             // mandatory
		false,             // immediate
		amqp.Publishing{
			ContentType:  BinaryDataMime,
			Body:         data,
			DeliveryMode: amqp.Persistent,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish content into exchange: %v", err)
	}
	return nil
}

func (q *ExchangeProducer) GetName() string {
	return q.name
}
