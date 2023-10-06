package main

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"strings"
)

const maxBatchLines = 1500
const defaultBatchLines = 300

// SaverConfig The configuration of the application
type SaverConfig struct {
	ID               string
	InputQueueName   string
	OutputFileNames  []string
	RabbitAddress    string
	GetterAddress    string
	GetterBatchLines uint
}

// InitEnv Initializes the configuration properties from a config file and environment
func InitEnv() (*viper.Viper, error) {
	v := viper.New()

	// Configure viper to read env variables with the CLI_ prefix
	v.AutomaticEnv()
	v.SetEnvPrefix("cli")
	// Use a replacer to replace env variables underscores with points. This let us
	// use nested configurations in the config file and at the same time define
	// env variables for the nested configurations
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Add env variables supported
	v.BindEnv("id")
	v.BindEnv("log", "level")
	v.BindEnv("rabbitmq", "address")
	v.BindEnv("rabbitmq", "queue", "input")
	v.BindEnv("saver", "output")
	v.BindEnv("getter", "address")
	v.BindEnv("getter", "batch", "lines")
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
func GetConfig(env *viper.Viper) (*SaverConfig, error) {
	id := env.GetString("id")
	if id == "" {
		return nil, errors.New("missing id")
	}

	inputQueueName := env.GetString("rabbitmq.queue.input")
	if inputQueueName == "" {
		return nil, errors.New("missing input queue")
	}

	outputFilenamesStr := env.GetString("saver.output")
	if inputQueueName == "" {
		return nil, errors.New("missing output filename")
	}
	outputs := strings.Split(outputFilenamesStr, ",")

	rabbitAddress := env.GetString("rabbitmq.address")
	if rabbitAddress == "" {
		return nil, errors.New("missing rabbitmq address")
	}

	getterAddress := env.GetString("getter.address")
	if getterAddress == "" {
		return nil, errors.New("missing getter address")
	}

	getterBatchLines := env.GetUint("getter.batch.lines")
	if getterBatchLines > maxBatchLines || getterBatchLines == 0 {
		log.Errorf("invalid getter batch lines. Setting to default")
		getterBatchLines = defaultBatchLines
	}

	log.Infof("action: config | result: success | id: %s | log_level: %s | rabbitAddress: %v | inputQueueName: %v | outputFilename: %v | getterAddress: %v | getterBatchLines: %v",
		id,
		env.GetString("log.level"),
		rabbitAddress,
		inputQueueName,
		outputFilenamesStr,
		getterAddress,
		getterBatchLines)

	return &SaverConfig{
		ID:               id,
		InputQueueName:   inputQueueName,
		OutputFileNames:  outputs,
		RabbitAddress:    rabbitAddress,
		GetterAddress:    getterAddress,
		GetterBatchLines: getterBatchLines,
	}, nil
}