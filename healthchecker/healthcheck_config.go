package main

import (
	"errors"
	"github.com/brunograssano/Distribuidos-TP1/common/config"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"strings"
)

// Config The configuration of the application
type Config struct {
	ID          string
	Address     string
	RestartTime uint
	CheckTime   uint
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
	_ = v.BindEnv("healthchecker", "address")
	_ = v.BindEnv("healthchecker", "restart", "time")
	_ = v.BindEnv("healthchecker", "check", "time")
	// Try to read configuration from config file. If config file
	// does not exist then ReadInConfig will fail but configuration
	// can be loaded from the environment variables, so we shouldn't
	// return an error in that case
	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		log.Warnf("DataProcesssorConfig | Warning Message | Configuration could not be read from config file. Using env variables instead")
	}

	return v, nil
}

// GetConfig Validates and returns the configuration of the application
func GetConfig(env *viper.Viper) (*Config, error) {
	if err := config.InitLogger(env.GetString("log.level")); err != nil {
		return nil, err
	}

	id := env.GetString("id")
	if id == "" {
		return nil, errors.New("missing id")
	}

	address := env.GetString("healthchecker.address")
	if address == "" {
		return nil, errors.New("missing address")
	}

	restartTime := env.GetUint("healthchecker.restart.time")
	if restartTime == 0 {
		return nil, errors.New("restartTime is missing or 0")
	}

	checkTime := env.GetUint("healthchecker.check.time")
	if restartTime == 0 {
		return nil, errors.New("checkTime is missing or 0")
	}

	log.Infof("HealthChecker Config | action: config | result: success | id: %s | log_level: %s | address: %v | restartTime: %v | checkTime: %v",
		id,
		env.GetString("log.level"),
		address,
		restartTime,
		checkTime,
	)

	return &Config{
		ID:          id,
		Address:     address,
		RestartTime: restartTime,
		CheckTime:   checkTime,
	}, nil
}
