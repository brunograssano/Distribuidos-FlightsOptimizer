package main

import (
	"filters_config"
	middleware "github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

func main() {
	sigs := utils.CreateSignalListener()
	env, err := filters_config.InitEnv()
	if err != nil {
		log.Fatalf("Main - Filter Distances | Error initializing env | %s", err)
	}
	filterDistanciaConfig, err := filters_config.GetConfigFilters(env)
	if err != nil {
		log.Fatalf("Main - Filter Distances | Error initializing config | %s", err)
	}

	qMiddleware := middleware.NewQueueMiddleware(filterDistanciaConfig.RabbitAddress)
	for i := 0; i < filterDistanciaConfig.GoroutinesCount; i++ {
		fd := NewFilterDistances(i, qMiddleware, filterDistanciaConfig)
		log.Infof("Main - Filter Distances | Spawning GoRoutine - Filter #%v", i)
		go fd.FilterDistances()
	}
	<-sigs
	qMiddleware.Close()
}
