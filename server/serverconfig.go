package main

import (
	"errors"
	"github.com/brunograssano/Distribuidos-TP1/common/config"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"strings"
)

type ServerConfig struct {
	ID                   string
	ServerAddress        string
	GetterAddresses      []string
	ExchangeNameAirports string
	ExchangeTypeAirports string
	ExchangeRKAirports   string
	QueueNameFlightRows  string
	RabbitAddress        string
}

func InitEnv() (*viper.Viper, error) {
	v := viper.New()

	v.AutomaticEnv()
	v.SetEnvPrefix("cli")

	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	_ = v.BindEnv("id")
	_ = v.BindEnv("log", "level")
	_ = v.BindEnv("server", "address")
	_ = v.BindEnv("getter", "addresses")
	_ = v.BindEnv("rabbitmq", "address")
	_ = v.BindEnv("queues", "airports", "exchange", "type")
	_ = v.BindEnv("queues", "airports", "exchange", "name")
	_ = v.BindEnv("queues", "airports", "exchange", "routingkey")
	_ = v.BindEnv("queues", "flightrows")

	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		log.Warnf("ServerConfig | Warning Message | Configuration could not be read from config file. Using env variables instead")
	}

	return v, nil
}

// GetConfig Validates and returns the configuration of the application
func GetConfig(env *viper.Viper) (*ServerConfig, error) {
	if err := config.InitLogger(env.GetString("log.level")); err != nil {
		return nil, err
	}

	id := env.GetString("id")
	if id == "" {
		return nil, errors.New("missing id")
	}

	serverAddress := env.GetString("server.address")
	if serverAddress == "" {
		return nil, errors.New("missing server address")
	}

	rabbitAddress := env.GetString("rabbitmq.address")
	if rabbitAddress == "" {
		return nil, errors.New("missing rabbit address")
	}

	getterAddressesStr := env.GetString("getter.addresses")
	if getterAddressesStr == "" {
		return nil, errors.New("missing getter address")
	}
	getterAddresses := strings.Split(getterAddressesStr, ",")

	typeExchangeAirports := env.GetString("queues.airports.exchange.type")
	if typeExchangeAirports == "" {
		return nil, errors.New("missing airports exchange type")
	}

	nameExchangeAirports := env.GetString("queues.airports.exchange.name")
	if nameExchangeAirports == "" {
		return nil, errors.New("missing airports exchange name")
	}

	rkExchangeAirports := env.GetString("queues.airports.exchange.routingkey")
	if rkExchangeAirports == "" {
		return nil, errors.New("missing airports exchange routing key")
	}

	flightRowQueueName := env.GetString("queues.flightrows")
	if flightRowQueueName == "" {
		return nil, errors.New("missing flight rows queue name")
	}

	log.Infof("ServerConfig | action: config | result: success | id: %s | log_level: %s | getterAddresses: %v | serverAddress: %v ",
		id,
		env.GetString("log.level"),
		getterAddresses,
		serverAddress,
	)

	return &ServerConfig{
		ID:                   id,
		ServerAddress:        serverAddress,
		GetterAddresses:      getterAddresses,
		ExchangeNameAirports: nameExchangeAirports,
		ExchangeTypeAirports: typeExchangeAirports,
		ExchangeRKAirports:   rkExchangeAirports,
		QueueNameFlightRows:  flightRowQueueName,
		RabbitAddress:        rabbitAddress,
	}, nil
}
