package main

import (
	"errors"
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/config"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"strconv"
	"strings"
)

// Config The configuration of the application
type Config struct {
	ID             string
	Address        string
	RestartTime    uint
	CheckTime      uint
	UdpAddress     []string
	NetAddresses   map[uint8][]string
	ElectionId     uint8
	HealthCheckers []string
	Name           string
	Containers     []string
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
	_ = v.BindEnv("name")
	_ = v.BindEnv("log", "level")
	_ = v.BindEnv("healthchecker", "address")
	_ = v.BindEnv("healthchecker", "addresses")
	_ = v.BindEnv("healthchecker", "restart", "time")
	_ = v.BindEnv("healthchecker", "check", "time")
	_ = v.BindEnv("healthchecker", "election", "id", "addresses")
	_ = v.BindEnv("healthchecker", "election", "udp", "address")
	_ = v.BindEnv("healthchecker", "election", "id")
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

	name := env.GetString("name")
	if name == "" {
		return nil, errors.New("missing name")
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

	udpAddressString := env.GetString("healthchecker.election.udp.address")
	if udpAddressString == "" {
		return nil, errors.New("missing udpAddress")
	}
	udpAddress := strings.Split(udpAddressString, ":")

	electionParticipantsIdsAndAddresses := env.GetString("healthchecker.election.id.addresses")
	electionParticipants := make(map[uint8][]string)
	if electionParticipantsIdsAndAddresses != "" {
		idsWithAddresses := strings.Split(electionParticipantsIdsAndAddresses, ",")
		for _, idAndAddress := range idsWithAddresses {
			idIpPort := strings.Split(idAndAddress, ":")
			idNodeStr := idIpPort[0]
			idNode, err := strconv.Atoi(idNodeStr)
			if err != nil || idNode > 255 {
				return nil, errors.New(fmt.Sprintf("election node id error when converting to int8: %v", err))
			}
			electionParticipants[uint8(idNode)] = idIpPort[1:]
		}
	} else {
		log.Warnf("HealthChecker Config | There is only one healthchecker")
	}

	myElectionId := env.GetUint("healthchecker.election.id")
	if myElectionId > 255 {
		return nil, errors.New(fmt.Sprintf("election id not valid: %v", myElectionId))
	}

	hcAddressesString := env.GetString("healthchecker.addresses")
	var hcAddresses []string
	if hcAddressesString == "" {
		log.Warnf("HealthChecker Config | There are no replicas of HealthCheckers to inform my health state")
	} else {
		hcAddresses = strings.Split(hcAddressesString, ",")
	}

	containersString := env.GetString("healthchecker.containers")
	var containers []string
	if containersString == "" {
		log.Warnf("HealthChecker Config | There are no containers using the Healthchecker initially")
	} else {
		containers = strings.Split(containersString, ",")
	}

	log.Infof("HealthChecker Config | action: config | result: success | id: %s | log_level: %s | address: %v | restartTime: %v | checkTime: %v | election id: %v | udpAddress: %v | networkAddresses: %v | otherHealthcheckers: %v | name: %v | containers: %v",
		id,
		env.GetString("log.level"),
		address,
		restartTime,
		checkTime,
		myElectionId,
		udpAddress,
		electionParticipantsIdsAndAddresses,
		hcAddresses,
		name,
		containers,
	)

	return &Config{
		ID:             id,
		Address:        address,
		RestartTime:    restartTime,
		CheckTime:      checkTime,
		UdpAddress:     udpAddress,
		NetAddresses:   electionParticipants,
		ElectionId:     uint8(myElectionId),
		HealthCheckers: hcAddresses,
		Name:           name,
		Containers:     containers,
	}, nil
}
