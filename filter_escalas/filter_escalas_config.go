package main

import (
	"errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type FilterEscalasConfig struct {
	ID               string
	InputQueueName   string
	OutputQueueNames []string
	GoroutinesCount  int
}

const maxGoroutines int = 32
const defaultGoroutines int = 4

func GetConfigFilterEscalas(env *viper.Viper) (*FilterEscalasConfig, error) {
	id := env.GetString("id")
	if id == "" {
		return nil, errors.New("missing id")
	}

	inputQueueName := env.GetString("filter.queues.input")
	if inputQueueName == "" {
		return nil, errors.New("missing input queue")
	}

	outputQueueNames := env.GetStringSlice("filter.queues.output")
	if len(outputQueueNames) <= 0 {
		return nil, errors.New("missing output queue")
	}

	goroutinesCount := env.GetInt("filter.goroutines")
	if goroutinesCount <= 0 || goroutinesCount > maxGoroutines {
		log.Warnf("Not a valid value '%v' for goroutines count, using default", goroutinesCount)
		goroutinesCount = defaultGoroutines
	}

	log.Infof("action: config | result: success | id: %s | log_level: %s | inputQueueNames: %v | outputQueueNames: %v | goroutinesCount: %v",
		id,
		env.GetString("log.level"),
		inputQueueName,
		outputQueueNames,
		goroutinesCount,
	)

	return &FilterEscalasConfig{
		ID:               id,
		InputQueueName:   inputQueueName,
		OutputQueueNames: outputQueueNames,
		GoroutinesCount:  goroutinesCount,
	}, nil
}
