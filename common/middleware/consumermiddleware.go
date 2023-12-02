package middleware

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
)

type Consumer struct {
	ConsumerInterface
	rabbitMQChannel     *amqp.Channel
	messageChannel      <-chan amqp.Delivery
	dlqMessageChannel   <-chan amqp.Delivery
	queue               amqp.Queue
	lastMessageConsumed *amqp.Delivery
}

func NewConsumer(channel *amqp.Channel, name string, durable bool, idDeadLetterQueue string) *Consumer {
	queue := CreateQueue(channel, name, durable)
	queueDeadLetter := CreateDeadLetterQueue(channel, idDeadLetterQueue)
	messages, err := channel.Consume(
		queue.Name, // queue
		"",         // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		map[string]interface{}{"x-dead-letter-exchange": "dead_letter_exchange"}, // Argumento para Dead Letter Exchange
	)
	messagesDLQ, err := channel.Consume(
		queueDeadLetter.Name, // queue
		"",                   // consumer
		false,                // auto-ack
		false,                // exclusive
		false,                // no-local
		false,                // no-wait
		nil,                  // args
	)
	//channel.
	FailOnError(err, "Failed to consume and create bytes channel in the RabbitMQ Queue.")
	return &Consumer{
		rabbitMQChannel:     channel,
		queue:               queue,
		messageChannel:      messages,
		dlqMessageChannel:   messagesDLQ,
		lastMessageConsumed: nil,
	}
}

func (queue *Consumer) Pop() ([]byte, bool) {
	select {
	case msg, ok := <-queue.messageChannel:
		queue.lastMessageConsumed = &msg
		return msg.Body, ok
	case msg, ok := <-queue.messageChannel:
		queue.lastMessageConsumed = &msg
		return msg.Body, ok
	}
}

func (queue *Consumer) BindTo(nameExchange string, routingKey string, kind string) error {
	err := queue.rabbitMQChannel.ExchangeDeclare(
		nameExchange,
		kind,
		true,
		false,
		false,
		false,
		nil,
	)
	FailOnError(err, fmt.Sprintf("Failed to declare the Exchange %v in RabbitMQ", nameExchange))
	err = queue.rabbitMQChannel.QueueBind(
		queue.queue.Name, // queue name
		routingKey,       // routing key
		nameExchange,     // exchange
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("error binding queue to exchange: %v", err)
	}
	return nil
}

func (queue *Consumer) SignalFinishedMessage(processedCorrectly bool) error {
	if queue.lastMessageConsumed != nil {
		var err error
		deliveredId := queue.lastMessageConsumed.DeliveryTag
		if !processedCorrectly {
			err = queue.rabbitMQChannel.Reject(deliveredId, true)
		} else {
			err = queue.rabbitMQChannel.Ack(deliveredId, false)
		}
		if err != nil {
			log.Errorf("Consumer | Error trying to send ACK/NACK to RabbitMQ | %v", err)
			return err
		}
		return nil
	}
	return nil
}

func (queue *Consumer) GetName() string {
	return queue.queue.Name
}
