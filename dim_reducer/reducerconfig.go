package main

import (
	"errors"
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/config"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"strings"
)

// ReducerConfig The configuration of the application
type ReducerConfig struct {
	ID              string
	InputQueueName  string
	OutputQueueName string
	ColumnsToKeep   []string
	GoroutinesCount int
	RabbitAddress   string
}

// ValueListSeparator Separator of queue list in configuration
const ValueListSeparator string = ","

const maxGoroutines int = 32
const defaultGoroutines int = 4

// initEnv Initializes the configuration properties from a config file and environment
func initEnv() (*viper.Viper, error) {
	v := viper.New()

	// Configure viper to read env variables with the CLI_ prefix
	v.AutomaticEnv()
	v.SetEnvPrefix("cli")
	// Use a replacer to replace env variables underscores with points. This let us
	// use nested configurations in the config file and at the same time define
	// env variables for the nested configurations
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Add env variables supported
	_ = v.BindEnv("id")
	_ = v.BindEnv("log", "level")
	_ = v.BindEnv("rabbitmq", "address")
	_ = v.BindEnv("rabbitmq", "queue", "input")
	_ = v.BindEnv("rabbitmq", "queue", "output")
	_ = v.BindEnv("reducer", "columns")
	_ = v.BindEnv("reducer", "goroutines")
	// Try to read configuration from config file. If config file
	// does not exist then ReadInConfig will fail but configuration
	// can be loaded from the environment variables, so we shouldn't
	// return an error in that case
	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Configuration could not be read from config file. Using env variables instead")
	}

	return v, nil
}

// GetConfig Validates and returns the configuration of the application
func GetConfig(env *viper.Viper) (*ReducerConfig, error) {
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

	columnsInList := env.GetString("reducer.columns")
	if columnsInList == "" {
		return nil, errors.New("missing columns to reduce dim")
	}

	rabbitAddress := env.GetString("rabbitmq.address")
	if rabbitAddress == "" {
		return nil, errors.New("missing rabbitmq address")
	}

	columnsToKeep := strings.Split(columnsInList, ValueListSeparator)

	goroutinesCount := env.GetInt("reducer.goroutines")
	if goroutinesCount <= 0 || goroutinesCount > maxGoroutines {
		log.Warnf("Not a valid value '%v' for goroutines count, using default", goroutinesCount)
		goroutinesCount = defaultGoroutines
	}

	log.Infof("action: config | result: success | id: %s | log_level: %s | rabbitAddress: %v | inputQueueName: %v | outputQueueName: %v | columnsToKeep: %v | goroutinesCount: %v",
		id,
		env.GetString("log.level"),
		rabbitAddress,
		inputQueueName, outputQueueName, columnsToKeep, goroutinesCount)

	return &ReducerConfig{
		ID:              id,
		InputQueueName:  inputQueueName,
		OutputQueueName: outputQueueName,
		ColumnsToKeep:   columnsToKeep,
		GoroutinesCount: goroutinesCount,
		RabbitAddress:   rabbitAddress,
	}, nil
}
