package main

import (
	"data_processor/processor"
	"github.com/brunograssano/Distribuidos-TP1/common/heartbeat"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/queuefactory"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

func main() {
	sigs := utils.CreateSignalListener()

	env, err := processor.InitEnv()
	if err != nil {
		log.Fatalf("Main - DataProcessor | Error initializing env | %s", err)
	}

	processorConfig, err := processor.GetConfig(env)
	if err != nil {
		log.Fatalf("Main - DataProcessor | Error initializing config | %s", err)
	}

	qMiddleware := middleware.NewQueueMiddleware(processorConfig.RabbitAddress)
	qFactory := queuefactory.NewSimpleQueueFactory(qMiddleware)
	for i := 0; i < processorConfig.GoroutinesCount; i++ {
		r := processor.NewDataProcessor(i, qFactory, processorConfig)
		go r.ProcessData()
	}
	endSignalHB := make(chan bool, 1)
	go heartbeat.HeartBeatLoop(processorConfig.AddressesHealthCheckers, processorConfig.ServiceName, uint32(processorConfig.HeartBeatTime), endSignalHB)
	<-sigs
	endSignalHB <- true
	qMiddleware.Close()
}
