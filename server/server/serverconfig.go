package server

import (
	"errors"
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/config"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"strings"
)

type ServerConfig struct {
	ID                      string
	ServerAddress           string
	GetterAddresses         map[uint8][]string
	ExchangeNameAirports    string
	ExchangeTypeAirports    string
	ExchangeRKAirports      string
	QueueNameFlightRows     string
	RabbitAddress           string
	ServiceName             string
	AddressesHealthCheckers []string
}

func InitEnv() (*viper.Viper, error) {
	v := viper.New()

	v.AutomaticEnv()
	v.SetEnvPrefix("cli")

	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	_ = v.BindEnv("id")
	_ = v.BindEnv("log", "level")
	_ = v.BindEnv("server", "address")
	_ = v.BindEnv("getter", "addresses", "1")
	_ = v.BindEnv("getter", "addresses", "2")
	_ = v.BindEnv("getter", "addresses", "3")
	_ = v.BindEnv("getter", "addresses", "4")
	_ = v.BindEnv("rabbitmq", "address")
	_ = v.BindEnv("queues", "airports", "exchange", "type")
	_ = v.BindEnv("queues", "airports", "exchange", "name")
	_ = v.BindEnv("queues", "airports", "exchange", "routingkey")
	_ = v.BindEnv("queues", "flightrows")
	_ = v.BindEnv("name")
	_ = v.BindEnv("healthchecker", "addresses")

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

	getterAddressesMap := make(map[uint8][]string)
	for i := uint8(1); i < 5; i++ {
		getterAddressesStr := env.GetString(fmt.Sprintf("getter.addresses.%v", i))
		if getterAddressesStr == "" {
			return nil, errors.New(fmt.Sprintf("missing getter address %v", i))
		}
		getterAddresses := strings.Split(getterAddressesStr, utils.CommaSeparator)
		getterAddressesMap[i] = getterAddresses
	}

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

	serviceName := env.GetString("name")
	if serviceName == "" {
		return nil, errors.New("missing name")
	}

	healthCheckerAddressesString := env.GetString("healthchecker.addresses")
	if healthCheckerAddressesString == "" {
		return nil, errors.New("missing healthchecker addresses")
	}
	healthCheckerAddresses := strings.Split(healthCheckerAddressesString, utils.CommaSeparator)

	log.Infof("ServerConfig | action: config | result: success | id: %s | log_level: %s | getterAddresses: %v | serverAddress: %v ",
		id,
		env.GetString("log.level"),
		getterAddressesMap,
		serverAddress,
	)

	return &ServerConfig{
		ID:                      id,
		ServerAddress:           serverAddress,
		GetterAddresses:         getterAddressesMap,
		ExchangeNameAirports:    nameExchangeAirports,
		ExchangeTypeAirports:    typeExchangeAirports,
		ExchangeRKAirports:      rkExchangeAirports,
		QueueNameFlightRows:     flightRowQueueName,
		RabbitAddress:           rabbitAddress,
		AddressesHealthCheckers: healthCheckerAddresses,
		ServiceName:             serviceName,
	}, nil
}
