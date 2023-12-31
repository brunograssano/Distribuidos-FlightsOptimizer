package config

import (
	"errors"
	"strings"

	"github.com/brunograssano/Distribuidos-TP1/common/config"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type CompleterConfig struct {
	ID                         string
	InputQueueAirportsName     string
	RoutingKeyExchangeAirports string
	ExchangeNameAirports       string
	ExchangeType               string
	InputQueueFlightsName      string
	OutputQueueName            string
	GoroutinesCount            int
	RabbitAddress              string
	AirportsFilename           string
	ServiceName                string
	AddressesHealthCheckers    []string
	TotalEofNodes              uint
}

func InitEnv() (*viper.Viper, error) {

	v := viper.New()

	v.AutomaticEnv()
	v.SetEnvPrefix("cli")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	_ = v.BindEnv("id")
	_ = v.BindEnv("log", "level")
	_ = v.BindEnv("rabbitmq", "queue", "input", "airport")
	_ = v.BindEnv("rabbitmq", "queue", "input", "flights")
	_ = v.BindEnv("rabbitmq", "queue", "output")
	_ = v.BindEnv("completer", "goroutines")
	_ = v.BindEnv("completer", "filename")
	_ = v.BindEnv("rabbitmq", "queue", "input", "airportroutingkey")
	_ = v.BindEnv("rabbitmq", "queue", "input", "airportexchange")
	_ = v.BindEnv("rabbitmq", "address")
	_ = v.BindEnv("queues", "airports", "exchange", "type")
	_ = v.BindEnv("name")
	_ = v.BindEnv("healthchecker", "addresses")
	_ = v.BindEnv("total", "nodes", "for", "eof")

	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		log.Warnf("DistCompleterConfig | Warning Message | Configuration could not be read from config file. Using env variables instead")
	}

	return v, nil
}

func GetConfig(env *viper.Viper) (*CompleterConfig, error) {
	if err := config.InitLogger(env.GetString("log.level")); err != nil {
		return nil, err
	}

	id := env.GetString("id")
	if id == "" {
		return nil, errors.New("missing id")
	}

	inputQueueAirportsName := env.GetString("rabbitmq.queue.input.airport")
	if inputQueueAirportsName == "" {
		return nil, errors.New("missing input queue for airports")
	}

	airportRoutingKey := env.GetString("rabbitmq.queue.input.airportroutingkey")
	if airportRoutingKey == "" {
		return nil, errors.New("missing input for airports routing key")
	}

	airportExchangeName := env.GetString("rabbitmq.queue.input.airportexchange")
	if airportExchangeName == "" {
		return nil, errors.New("missing input for airports exchange name")
	}

	inputQueueFlightsName := env.GetString("rabbitmq.queue.input.flights")
	if inputQueueAirportsName == "" {
		return nil, errors.New("missing input queue for flights")
	}

	outputQueueName := env.GetString("rabbitmq.queues.output")
	if outputQueueName == "" {
		return nil, errors.New("missing output queue")
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

	goroutinesCount := env.GetInt("completer.goroutines")
	if goroutinesCount <= 0 || goroutinesCount > utils.MaxGoroutines {
		log.Warnf("DistCompleterConfig | Not a valid value '%v' for goroutines count, using default", goroutinesCount)
		goroutinesCount = utils.DefaultGoroutines
	}

	exchangeType := env.GetString("queues.airports.exchange.type")
	if exchangeType == "" {
		return nil, errors.New("missing exchangeType")
	}

	fileName := env.GetString("completer.filename")
	if fileName == "" {
		return nil, errors.New("missing filename")
	}

	TotalEofNodes := env.GetUint("total.nodes.for.eof")
	if TotalEofNodes == 0 {
		return nil, errors.New("missing total nodes for eof")
	}

	log.Infof("DistCompleterConfig | action: config | result: success | id: %s | log_level: %s | inputQueueAirportName: %v | inputQueueFlightName: %v | outputQueueNames: %v | goroutinesCount: %v | airportsFilename: %v | exchangeName: %v | routingKey: %v",
		id,
		env.GetString("log.level"),
		inputQueueAirportsName,
		inputQueueFlightsName,
		outputQueueName,
		goroutinesCount,
		fileName,
		airportExchangeName,
		airportRoutingKey,
	)

	return &CompleterConfig{
		ID:                         id,
		InputQueueAirportsName:     inputQueueAirportsName,
		InputQueueFlightsName:      inputQueueFlightsName,
		OutputQueueName:            outputQueueName,
		GoroutinesCount:            goroutinesCount,
		RabbitAddress:              rabbitAddress,
		AirportsFilename:           fileName,
		ExchangeNameAirports:       airportExchangeName,
		ExchangeType:               exchangeType,
		RoutingKeyExchangeAirports: airportRoutingKey,
		AddressesHealthCheckers:    healthCheckerAddresses,
		ServiceName:                serviceName,
		TotalEofNodes:              TotalEofNodes,
	}, nil
}
