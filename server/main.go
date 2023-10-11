package main

import (
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

func main() {
	sigs := utils.CreateSignalListener()
	env, err := InitEnv()
	if err != nil {
		log.Fatalf("Main - Server | Error initializing env | %s", err)
	}
	serverConfig, err := GetConfig(env)
	if err != nil {
		log.Fatalf("Main - Server | Error initializing config | %s", err)
	}
	server := NewServer(serverConfig)
	go server.StartServerLoop()
	log.Infof("Main - Server | Spawned Server...")
	<-sigs
	log.Infof("Main - Server | Ending Server...")
	_ = server.End()
}
