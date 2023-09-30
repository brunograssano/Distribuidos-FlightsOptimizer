package filters_config

import (
	"errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"strings"
)

type FilterConfig struct {
	ID               string
	InputQueueName   string
	OutputQueueNames []string
	GoroutinesCount  int
	RabbitAddress    string
}

const ValueListSeparator string = ","
const maxGoroutines int = 32
const defaultGoroutines int = 4

func GetConfigFilters(env *viper.Viper) (*FilterConfig, error) {
	id := env.GetString("id")
	if id == "" {
		return nil, errors.New("missing id")
	}

	inputQueueName := env.GetString("rabbitmq.queues.input")
	if inputQueueName == "" {
		return nil, errors.New("missing input queue")
	}

	outputQueueNames := env.GetString("rabbitmq.queues.output")
	if len(outputQueueNames) <= 0 {
		return nil, errors.New("missing output queue")
	}
	outputQueueNamesArray := strings.Split(outputQueueNames, ValueListSeparator)

	rabbitAddress := env.GetString("rabbitmq.address")
	if rabbitAddress == "" {
		return nil, errors.New("missing rabbitmq address")
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

	return &FilterConfig{
		ID:               id,
		InputQueueName:   inputQueueName,
		OutputQueueNames: outputQueueNamesArray,
		GoroutinesCount:  goroutinesCount,
		RabbitAddress:    rabbitAddress,
	}, nil
}
