package main

import (
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	"log"
	"saver_ex_3/ex3"
)

func main() {
	sigs := utils.CreateSignalListener()
	env, err := ex3.InitEnv()
	if err != nil {
		log.Fatalf("Main - Saver Ex3 | Error initializing env | %s", err)
	}

	config, err := ex3.GetConfig(env)
	if err != nil {
		log.Fatalf("Main - Saver Ex3 | Error initializing config | %s", err)
	}
	qMiddleware := middleware.NewQueueMiddleware(config.RabbitAddress)

	saverEx3 := ex3.NewEx3Handler(config)
	go saverEx3.StartHandler()
	//go heartbeat.HeartBeatLoop(config.HealthCheckerAddresses)
	<-sigs
	qMiddleware.Close()
	saverEx3.Close()
}
