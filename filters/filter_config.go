package filters_config

import (
	"errors"
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/config"
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

func InitEnv() (*viper.Viper, error) {
	v := viper.New()

	v.AutomaticEnv()
	v.SetEnvPrefix("cli")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	_ = v.BindEnv("id")
	_ = v.BindEnv("log", "level")
	_ = v.BindEnv("rabbitmq", "queues", "input")
	_ = v.BindEnv("rabbitmq", "queues", "output")
	_ = v.BindEnv("filter", "goroutines")
	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Configuration could not be read from config file. Using env variables instead")
	}

	return v, nil
}

func GetConfigFilters(env *viper.Viper) (*FilterConfig, error) {
	if err := config.InitLogger(env.GetString("log.level")); err != nil {
		return nil, err
	}

	id := env.GetString("id")
	if id == "" {
		return nil, errors.New("missing id")
	}

	inputQueueName := env.GetString("rabbitmq.queues.input")
	if inputQueueName == "" {
		return nil, errors.New("missing input queue")
	}

	outputQueueNames := env.GetString("rabbitmq.queues.output")
	if outputQueueNames == "" {
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
		outputQueueNamesArray,
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
