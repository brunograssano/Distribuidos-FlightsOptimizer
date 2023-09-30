package main

import (
	"errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"strings"
)

type ProcessorConfig struct {
	ID                   string
	InputQueueName       string
	OutputQueueNameEx123 []string
	OutputQueueNameEx4   string
	GoroutinesCount      int
	RabbitAddress        string
}

const ValueListSeparator string = ","
const maxGoroutines int = 32
const defaultGoroutines int = 4

func GetConfig(env *viper.Viper) (*ProcessorConfig, error) {
	id := env.GetString("id")
	if id == "" {
		return nil, errors.New("missing id")
	}

	inputQueueName := env.GetString("rabbitmq.queue.input")
	if inputQueueName == "" {
		return nil, errors.New("missing input queue")
	}

	outputQueueNameEx123 := env.GetString("rabbitmq.queue.output.ex123")
	if outputQueueNameEx123 == "" {
		return nil, errors.New("missing output queues for ex 1, 2, 3")
	}
	outputQueueNameEx123Array := strings.Split(outputQueueNameEx123, ValueListSeparator)

	outputQueueNameEx4 := env.GetString("rabbitmq.queue.output.ex4")
	if outputQueueNameEx4 == "" {
		return nil, errors.New("missing output queue for ex 4")
	}

	rabbitAddress := env.GetString("rabbitmq.address")
	if rabbitAddress == "" {
		return nil, errors.New("missing rabbitmq address")
	}

	goroutinesCount := env.GetInt("producer.goroutines")
	if goroutinesCount <= 0 || goroutinesCount > maxGoroutines {
		log.Warnf("Not a valid value '%v' for goroutines count, using default", goroutinesCount)
		goroutinesCount = defaultGoroutines
	}

	log.Infof("action: config | result: success | id: %s | log_level: %s | rabbitAddress: %v | inputQueueName: %v | outputQueueNameEx123: %v | outputQueueNameEx4: %v | goroutinesCount: %v",
		id,
		env.GetString("log.level"),
		rabbitAddress,
		inputQueueName, outputQueueNameEx123, outputQueueNameEx4, goroutinesCount)

	return &ProcessorConfig{
		ID:                   id,
		InputQueueName:       inputQueueName,
		OutputQueueNameEx123: outputQueueNameEx123Array,
		OutputQueueNameEx4:   outputQueueNameEx4,
		GoroutinesCount:      goroutinesCount,
		RabbitAddress:        rabbitAddress,
	}, nil
}
