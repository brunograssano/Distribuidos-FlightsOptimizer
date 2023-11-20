package main

import (
	"errors"
	"github.com/brunograssano/Distribuidos-TP1/common/config"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"strings"
)

// Ex4JourneySaverConfig The configuration of the application
type Ex4JourneySaverConfig struct {
	ID                      string
	InputQueueName          string
	OutputQueueNameAccum    string
	OutputQueueNameSaver    string
	RabbitAddress           string
	InternalSaversCount     uint
	RoutingKeyInput         uint
	ServiceName             string
	AddressesHealthCheckers []string
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
	_ = v.BindEnv("rabbitmq", "queue", "outputs", "accum")
	_ = v.BindEnv("rabbitmq", "queue", "outputs", "saver")
	_ = v.BindEnv("internal", "savers", "count")
	_ = v.BindEnv("rabbitmq", "rk", "input")
	_ = v.BindEnv("name")
	_ = v.BindEnv("healthchecker", "addresses")

	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		log.Warnf("Ex4JourneySaverConfig | Warning Message | Configuration could not be read from config file. Using env variables instead")
	}

	return v, nil
}

// GetConfig Validates and returns the configuration of the application
func GetConfig(env *viper.Viper) (*Ex4JourneySaverConfig, error) {
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

	outputQueueNameAccum := env.GetString("rabbitmq.queue.outputs.accum")
	if outputQueueNameAccum == "" {
		return nil, errors.New("missing output queue for accum")
	}

	outputQueueNameSaver := env.GetString("rabbitmq.queue.outputs.saver")
	if outputQueueNameSaver == "" {
		return nil, errors.New("missing output queue for accum")
	}

	rabbitAddress := env.GetString("rabbitmq.address")
	if rabbitAddress == "" {
		return nil, errors.New("missing rabbitmq address")
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

	rkInput := env.GetUint("rabbitmq.rk.input")

	internalSaversCount := env.GetUint("internal.savers.count")
	if internalSaversCount <= 0 || internalSaversCount > utils.MaxGoroutines {
		log.Warnf("Ex4JourneySaverConfig | Not a valid value '%v' for internal savers count, using default", internalSaversCount)
		internalSaversCount = utils.DefaultGoroutines
	}

	if err := config.InitLogger(env.GetString("log.level")); err != nil {
		return nil, err
	}

	log.Infof("Ex4JourneySaverConfig | action: config | result: success | id: %s | log_level: %s | rabbitAddress: %v | inputQueueName: %v | internalSaversCount: %v | routingKey: %v",
		id,
		env.GetString("log.level"),
		rabbitAddress,
		inputQueueName,
		internalSaversCount,
		rkInput)

	return &Ex4JourneySaverConfig{
		ID:                      id,
		InputQueueName:          inputQueueName,
		OutputQueueNameAccum:    outputQueueNameAccum,
		OutputQueueNameSaver:    outputQueueNameSaver,
		RabbitAddress:           rabbitAddress,
		InternalSaversCount:     internalSaversCount,
		RoutingKeyInput:         rkInput,
		AddressesHealthCheckers: healthCheckerAddresses,
		ServiceName:             serviceName,
	}, nil
}
