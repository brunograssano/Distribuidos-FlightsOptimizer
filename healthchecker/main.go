package main

import (
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

func main() {
	sigs := utils.CreateSignalListener()
	env, err := InitEnv()
	if err != nil {
		log.Fatalf("Main - Health Checker | Error initializing env | %s", err)
	}

	config, err := GetConfig(env)
	if err != nil {
		log.Fatalf("Main - Health Checker | Error initializing config | %s", err)
	}

	h := NewHealthChecker(config)
	go h.HandleHeartBeats()
	<-sigs
	h.Close()
}
