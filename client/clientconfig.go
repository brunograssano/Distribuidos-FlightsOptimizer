package main

import (
	"errors"
	"github.com/brunograssano/Distribuidos-TP1/common/config"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"strings"
)

const DefaultBatchSize = 300

// ClientConfig The configuration of the application
type ClientConfig struct {
	ID              string
	InputFileName   string
	AirportFileName string
	ServerAddress   string
	Batch           uint
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
	_ = v.BindEnv("id")
	_ = v.BindEnv("log", "level")
	_ = v.BindEnv("input", "file")
	_ = v.BindEnv("input", "airports")
	_ = v.BindEnv("input", "batch")
	_ = v.BindEnv("server", "address")
	// Try to read configuration from config file. If config file
	// does not exist then ReadInConfig will fail but configuration
	// can be loaded from the environment variables, so we shouldn't
	// return an error in that case
	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		log.Warnf("Client Config | Configuration could not be read from config file. Using env variables instead")
	}

	return v, nil
}

// GetConfig Validates and returns the configuration of the application
func GetConfig(env *viper.Viper) (*ClientConfig, error) {
	if err := config.InitLogger(env.GetString("log.level")); err != nil {
		return nil, err
	}

	id := env.GetString("id")
	if id == "" {
		return nil, errors.New("missing id")
	}

	inputFile := env.GetString("input.file")
	if inputFile == "" {
		return nil, errors.New("missing input file")
	}

	inputAirports := env.GetString("input.airports")
	if inputAirports == "" {
		return nil, errors.New("missing airports file")
	}

	serverAddress := env.GetString("server.address")
	if serverAddress == "" {
		return nil, errors.New("missing server address")
	}

	batch := env.GetUint("input.batch")
	if batch == 0 {
		log.Warnf("Client Config | Warning Message | Missing batch size, using default")
		batch = DefaultBatchSize
	}

	log.Infof("Client Config | action: config | result: success | id: %s | log_level: %s | inputFile: %v | serverAddress: %v | inputAirports: %v | batch: %v",
		id,
		env.GetString("log.level"),
		inputFile,
		serverAddress,
		inputAirports,
		batch)

	return &ClientConfig{
		ID:              id,
		InputFileName:   inputFile,
		ServerAddress:   serverAddress,
		AirportFileName: inputAirports,
		Batch:           batch,
	}, nil
}
