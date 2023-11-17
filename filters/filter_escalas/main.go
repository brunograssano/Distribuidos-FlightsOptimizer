package main

import (
	"filters_config"
	"github.com/brunograssano/Distribuidos-TP1/common/heartbeat"
	middleware "github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/queuefactory"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

func main() {
	sigs := utils.CreateSignalListener()
	env, err := filters_config.InitEnv()
	if err != nil {
		log.Fatalf("Main - Filter Stopovers | Error initializing env | %s", err)
	}
	config, err := filters_config.GetConfigFilters(env)
	if err != nil {
		log.Fatalf("Main - Filter Stopovers | Error initializing config | %s", err)
	}

	qMiddleware := middleware.NewQueueMiddleware(config.RabbitAddress)
	qFactory := queuefactory.NewSimpleQueueFactory(qMiddleware)
	for i := 0; i < config.GoroutinesCount; i++ {
		fe := NewFilterStopovers(i, qFactory, config)
		log.Infof("Main - Filter Stopovers | Spawning GoRoutine - Filter #%v", i)
		go fe.FilterStopovers()
	}
	endSigHB := heartbeat.StartHeartbeat(config.AddressesHealthCheckers, config.ServiceName)
	<-sigs
	endSigHB <- true
	qMiddleware.Close()
}
