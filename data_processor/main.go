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

	config, err := processor.GetConfig(env)
	if err != nil {
		log.Fatalf("Main - DataProcessor | Error initializing config | %s", err)
	}

	qMiddleware := middleware.NewQueueMiddleware(config.RabbitAddress)
	qFactory := queuefactory.NewSimpleQueueFactory(qMiddleware)
	for i := 0; i < config.GoroutinesCount; i++ {
		r := processor.NewDataProcessor(i, qFactory, config)
		go r.ProcessData()
	}
	endSigHB := heartbeat.StartHeartbeat(config.AddressesHealthCheckers, config.ServiceName)
	<-sigs
	endSigHB <- true
	qMiddleware.Close()
}
