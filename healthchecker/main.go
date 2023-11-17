package main

import (
	"github.com/brunograssano/Distribuidos-TP1/common/heartbeat"
	"github.com/brunograssano/Distribuidos-TP1/common/leader"
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
	electionService := leader.NewLeaderElectionService(config.ElectionId, config.NetAddresses, config.UdpAddress)
	go electionService.ReceiveNetMessages()
	h := NewHealthChecker(config, electionService)
	go h.HandleHeartBeats()
	endSigHB := make(chan bool, 1)
	go heartbeat.HeartBeatLoop(config.HealthCheckers, config.Name, utils.TimePerHeartbeat, endSigHB)
	<-sigs
	endSigHB <- true
	h.Close()
}
