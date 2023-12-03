package filters_config

import (
	"errors"
	"strings"

	"github.com/brunograssano/Distribuidos-TP1/common/config"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type FilterConfig struct {
	ID                      string
	InputQueueName          string
	OutputQueueNames        []string
	OutputExchangeNames     []string
	GoroutinesCount         int
	RabbitAddress           string
	AddressesHealthCheckers []string
	ServiceName             string
	TotalEofNodes           uint
}

func InitEnv() (*viper.Viper, error) {
	v := viper.New()

	v.AutomaticEnv()
	v.SetEnvPrefix("cli")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	_ = v.BindEnv("id")
	_ = v.BindEnv("log", "level")
	_ = v.BindEnv("rabbitmq", "queues", "input")
	_ = v.BindEnv("rabbitmq", "queues", "output")
	_ = v.BindEnv("rabbitmq", "exchange", "outputs")
	_ = v.BindEnv("filter", "goroutines")
	_ = v.BindEnv("name")
	_ = v.BindEnv("healthchecker", "addresses")
	_ = v.BindEnv("total", "nodes", "for", "eof")

	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		log.Warnf("FilterConfig | Warning Message | Configuration could not be read from config file. Using env variables instead")
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
	outputQueueNamesArray := strings.Split(outputQueueNames, utils.CommaSeparator)

	outputExchangesNames := env.GetString("rabbitmq.exchange.outputs")
	var outputExchangesNamesArray []string
	if outputQueueNames == "" {
		log.Warnf("Missing output exchanges. Setting it as empty array")
		outputExchangesNamesArray = []string{}
	} else {
		outputExchangesNamesArray = strings.Split(outputExchangesNames, utils.CommaSeparator)
	}

	rabbitAddress := env.GetString("rabbitmq.address")
	if rabbitAddress == "" {
		return nil, errors.New("missing rabbitmq address")
	}

	TotalEofNodes := env.GetUint("total.nodes.for.eof")
	if TotalEofNodes == 0 {
		return nil, errors.New("missing total nodes for eof")
	}

	goroutinesCount := env.GetInt("filter.goroutines")
	if goroutinesCount <= 0 || goroutinesCount > utils.MaxGoroutines {
		log.Warnf("FilterConfig | Warn Message | Not a valid value '%v' for goroutines count, using default", goroutinesCount)
		goroutinesCount = utils.DefaultGoroutines
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

	log.Infof("FilterConfig | action: config | result: success | id: %s | log_level: %s | inputQueueNames: %v | outputQueueNames: %v | outputExchangesNames: %v | goroutinesCount: %v",
		id,
		env.GetString("log.level"),
		inputQueueName,
		outputQueueNamesArray,
		outputExchangesNamesArray,
		goroutinesCount,
	)

	return &FilterConfig{
		ID:                      id,
		InputQueueName:          inputQueueName,
		OutputQueueNames:        outputQueueNamesArray,
		OutputExchangeNames:     outputExchangesNamesArray,
		GoroutinesCount:         goroutinesCount,
		RabbitAddress:           rabbitAddress,
		AddressesHealthCheckers: healthCheckerAddresses,
		ServiceName:             serviceName,
		TotalEofNodes:           TotalEofNodes,
	}, nil
}
