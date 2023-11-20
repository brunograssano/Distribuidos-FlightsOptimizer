package main

import (
	"errors"
	"github.com/brunograssano/Distribuidos-TP1/common/config"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"strings"
)

// SinkConfig The configuration of the application
type SinkConfig struct {
	ID                      string
	InputQueueName          string
	OutputQueueName         string
	RabbitAddress           string
	SaversCount             uint
	AddressesHealthCheckers []string
	ServiceName             string
}

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
	_ = v.BindEnv("savers", "count")
	_ = v.BindEnv("name")
	_ = v.BindEnv("healthchecker", "addresses")

	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		log.Warnf("SinkConfig | Warning Message | Configuration could not be read from config file. Using env variables instead")
	}

	return v, nil
}

// GetConfig Validates and returns the configuration of the application
func GetConfig(env *viper.Viper) (*SinkConfig, error) {
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

	saversCount := env.GetUint("savers.count")
	if saversCount <= 0 {
		return nil, errors.New("invalid savers count")
	}

	serviceName := env.GetString("name")
	if serviceName == "" {
		return nil, errors.New("missing name")
	}

	healthCheckerAddressesString := env.GetString("healthchecker.addresses")
	if healthCheckerAddressesString == "" {
		return nil, errors.New("missing healthchecker addresses")
	}
	healthCheckerAddresses := strings.Split(healthCheckerAddressesString, utils.CommaSeparator)

	if err := config.InitLogger(env.GetString("log.level")); err != nil {
		return nil, err
	}

	log.Infof("SinkConfig | action: config | result: success | id: %s | log_level: %s | rabbitAddress: %v | inputQueueName: %v ",
		id,
		env.GetString("log.level"),
		rabbitAddress,
		inputQueueName)

	return &SinkConfig{
		ID:                      id,
		InputQueueName:          inputQueueName,
		OutputQueueName:         outputQueueName,
		RabbitAddress:           rabbitAddress,
		SaversCount:             saversCount,
		AddressesHealthCheckers: healthCheckerAddresses,
		ServiceName:             serviceName,
	}, nil
}
