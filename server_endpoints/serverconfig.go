package server_endpoints

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"strings"
)

type ServerConfig struct {
	ID              string
	ServerAddress   string
	GetterAddresses []string
}

func InitEnv() (*viper.Viper, error) {
	v := viper.New()

	v.AutomaticEnv()
	v.SetEnvPrefix("cli")

	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	_ = v.BindEnv("id")
	_ = v.BindEnv("log", "level")
	_ = v.BindEnv("server", "address")
	_ = v.BindEnv("getter", "address")

	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Configuration could not be read from config file. Using env variables instead")
	}

	return v, nil
}

// GetConfig Validates and returns the configuration of the application
func GetConfig(env *viper.Viper) (*ServerConfig, error) {
	id := env.GetString("id")
	if id == "" {
		return nil, errors.New("missing id")
	}

	inputFile := env.GetString("input.file")
	if inputFile == "" {
		return nil, errors.New("missing input file")
	}

	inputAirports := env.GetString("input.file")
	if inputAirports == "" {
		return nil, errors.New("missing airports file")
	}

	serverAddress := env.GetString("server.address")
	if serverAddress == "" {
		return nil, errors.New("missing server address")
	}

	getterAddressesStr := env.GetString("getter.address")
	if getterAddressesStr == "" {
		return nil, errors.New("missing server address")
	}
	getterAddresses := strings.Split(getterAddressesStr, ",")

	log.Infof("action: config | result: success | id: %s | log_level: %s | getterAddresses: %v | serverAddress: %v ",
		id,
		env.GetString("log.level"),
		getterAddresses,
		serverAddress,
	)

	return &ServerConfig{
		ID:              id,
		ServerAddress:   serverAddress,
		GetterAddresses: getterAddresses,
	}, nil
}
