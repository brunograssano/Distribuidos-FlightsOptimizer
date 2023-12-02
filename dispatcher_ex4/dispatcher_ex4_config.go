package main

import (
	"errors"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	"strings"

	"github.com/brunograssano/Distribuidos-TP1/common/config"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// DispatcherEx4Config The configuration of the application
type DispatcherEx4Config struct {
	ID                      string
	InputQueueName          string
	OutputExchangeName      string
	RabbitAddress           string
	SaversCount             uint
	DispatchersCount        uint
	ServiceName             string
	AddressesHealthCheckers []string
	TotalEofNodes           uint
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
	_ = v.BindEnv("internal", "dispatcher", "count")
	_ = v.BindEnv("name")
	_ = v.BindEnv("healthchecker", "addresses")
	_ = v.BindEnv("total", "nodes", "for", "eof")

	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		log.Warnf("DispatcherEx4Config | Warning Message | Configuration could not be read from config file. Using env variables instead")
	}

	return v, nil
}

// GetConfig Validates and returns the configuration of the application
func GetConfig(env *viper.Viper) (*DispatcherEx4Config, error) {
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

	outputExchangeName := env.GetString("rabbitmq.queue.output")
	if outputExchangeName == "" {
		return nil, errors.New("missing output queue")
	}

	rabbitAddress := env.GetString("rabbitmq.address")
	if rabbitAddress == "" {
		return nil, errors.New("missing rabbitmq address")
	}

	saversCount := env.GetUint("savers.count")
	if saversCount <= 0 {
		return nil, errors.New("invalid handlers count")
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

	TotalEofNodes := env.GetUint("total.nodes.for.eof")
	if TotalEofNodes == 0 {
		return nil, errors.New("missing total nodes for eof")
	}

	internalDispatcherCount := env.GetUint("internal.dispatcher.count")
	if internalDispatcherCount <= 0 || internalDispatcherCount > utils.MaxGoroutines {
		log.Warnf("DispatcherEx4Config | Not a valid value '%v' for internal dispatchers count, using default", internalDispatcherCount)
		internalDispatcherCount = utils.DefaultGoroutines
	}

	if err := config.InitLogger(env.GetString("log.level")); err != nil {
		return nil, err
	}

	log.Infof("DispatcherEx4Config | action: config | result: success | id: %s | log_level: %s | rabbitAddress: %v | inputQueueName: %v | saversCount: %v",
		id,
		env.GetString("log.level"),
		rabbitAddress,
		inputQueueName,
		saversCount)

	return &DispatcherEx4Config{
		ID:                      id,
		InputQueueName:          inputQueueName,
		OutputExchangeName:      outputExchangeName,
		RabbitAddress:           rabbitAddress,
		SaversCount:             saversCount,
		DispatchersCount:        internalDispatcherCount,
		AddressesHealthCheckers: healthCheckerAddresses,
		ServiceName:             serviceName,
		TotalEofNodes:           TotalEofNodes,
	}, nil
}
