package main

import (
	"errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"strings"
)

type ReducerConfig struct {
	ID              string
	InputQueueName  string
	OutputQueueName string
	ColumnsToKeep   []string
	GoroutinesCount int
	RabbitAddress   string
}

const ValueListSeparator string = ","
const maxGoroutines int = 32
const defaultGoroutines int = 4

func GetConfig(env *viper.Viper) (*ReducerConfig, error) {
	id := env.GetString("id")
	if id == "" {
		return nil, errors.New("missing id")
	}

	inputQueueName := env.GetString("rabbitmq.queue.input")
	if inputQueueName == "" {
		return nil, errors.New("missing input queue")
	}

	outputQueueName := env.GetString("rabbitmq.queue.output")
	if outputQueueName == "" {
		return nil, errors.New("missing output queue")
	}

	columnsInList := env.GetString("reducer.columns")
	if columnsInList == "" {
		return nil, errors.New("missing columns to reduce dim")
	}

	rabbitAddress := env.GetString("rabbitmq.address")
	if rabbitAddress == "" {
		return nil, errors.New("missing rabbitmq address")
	}

	columnsToKeep := strings.Split(columnsInList, ValueListSeparator)

	goroutinesCount := env.GetInt("reducer.goroutines")
	if goroutinesCount <= 0 || goroutinesCount > maxGoroutines {
		log.Warnf("Not a valid value '%v' for goroutines count, using default", goroutinesCount)
		goroutinesCount = defaultGoroutines
	}

	log.Infof("action: config | result: success | id: %s | log_level: %s | rabbitAddress: %v | inputQueueName: %v | outputQueueName: %v | columnsToKeep: %v | goroutinesCount: %v",
		id,
		env.GetString("log.level"),
		rabbitAddress,
		inputQueueName, outputQueueName, columnsToKeep, goroutinesCount)

	return &ReducerConfig{
		ID:              id,
		InputQueueName:  inputQueueName,
		OutputQueueName: outputQueueName,
		ColumnsToKeep:   columnsToKeep,
		GoroutinesCount: goroutinesCount,
		RabbitAddress:   rabbitAddress,
	}, nil
}
