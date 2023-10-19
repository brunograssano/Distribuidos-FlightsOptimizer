package main

import (
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"server/server"
)

func main() {
	sigs := utils.CreateSignalListener()
	env, err := server.InitEnv()
	if err != nil {
		log.Fatalf("Main - Server | Error initializing env | %s", err)
	}
	serverConfig, err := server.GetConfig(env)
	if err != nil {
		log.Fatalf("Main - Server | Error initializing config | %s", err)
	}
	s := server.NewServer(serverConfig)
	go s.StartServerLoop()
	log.Infof("Main - Server | Spawned Server...")
	<-sigs
	log.Infof("Main - Server | Ending Server...")
	_ = s.End()
}
