package main

import (
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

func main() {
	sigs := utils.CreateSignalListener()
	env, err := InitEnv()
	if err != nil {
		log.Fatalf("%s", err)
	}
	serverConfig, err := GetConfig(env)
	if err != nil {
		log.Fatalf("%s", err)
	}
	server := NewServer(serverConfig)
	go server.StartServerLoop()
	log.Infof("Spawned Server...")
	<-sigs
	log.Infof("Ending Server...")
	_ = server.End()
}
