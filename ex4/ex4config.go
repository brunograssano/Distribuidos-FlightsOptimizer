package main

import (
	"errors"
	"github.com/brunograssano/Distribuidos-TP1/common/config"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"strings"
)

// Ex4Config The configuration of the application
type Ex4Config struct {
	ID                  string
	InputQueueName      string
	OutputQueueName     string
	RabbitAddress       string
	InternalSaversCount uint
}

const maxSaversCount = 32
const defaultSaversCount = 4

// InitEnv Initializes the configuration properties from a config file and environment
func InitEnv() (*viper.Viper, error) {
	v := viper.New()

	v.AutomaticEnv()
	v.SetEnvPrefix("cli")

	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	_ = v.BindEnv("id")
	_ = v.BindEnv("log", "level")
	_ = v.BindEnv("rabbitmq", "address")
	_ = v.BindEnv("rabbitmq", "queue", "input")
	_ = v.BindEnv("rabbitmq", "queue", "output")
	_ = v.BindEnv("internal", "savers", "count")

	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		log.Warnf("Ex4Config | Warning Message | Configuration could not be read from config file. Using env variables instead")
	}

	return v, nil
}

// GetConfig Validates and returns the configuration of the application
func GetConfig(env *viper.Viper) (*Ex4Config, error) {
	if err := config.InitLogger(env.GetString("log.level")); err != nil {
		return nil, err
	}

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

	rabbitAddress := env.GetString("rabbitmq.address")
	if rabbitAddress == "" {
		return nil, errors.New("missing rabbitmq address")
	}

	internalSaversCount := env.GetUint("internal.savers.count")
	if internalSaversCount <= 0 || internalSaversCount > maxSaversCount {
		log.Warnf("Ex4Config | Not a valid value '%v' for internal savers count, using default", internalSaversCount)
		internalSaversCount = defaultSaversCount
	}

	if err := config.InitLogger(env.GetString("log.level")); err != nil {
		return nil, err
	}

	log.Infof("Ex4Config | action: config | result: success | id: %s | log_level: %s | rabbitAddress: %v | inputQueueName: %v | internalSaversCount: %v",
		id,
		env.GetString("log.level"),
		rabbitAddress,
		inputQueueName,
		internalSaversCount)

	return &Ex4Config{
		ID:                  id,
		InputQueueName:      inputQueueName,
		OutputQueueName:     outputQueueName,
		RabbitAddress:       rabbitAddress,
		InternalSaversCount: internalSaversCount,
	}, nil
}
