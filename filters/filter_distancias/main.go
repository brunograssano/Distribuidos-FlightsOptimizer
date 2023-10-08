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
		log.Fatalf("%s", err)
	}
	filterDistanciaConfig, err := filters_config.GetConfigFilters(env)
	if err != nil {
		log.Fatalf("%s", err)
	}

	qMiddleware := middleware.NewQueueMiddleware(filterDistanciaConfig.RabbitAddress)
	for i := 0; i < filterDistanciaConfig.GoroutinesCount; i++ {
		fd := NewFilterDistances(i, qMiddleware, filterDistanciaConfig)
		log.Infof("Spawning GoRoutine #%v - Filter Escalas", i)
		go fd.FilterDistances()
	}
	<-sigs
	qMiddleware.Close()
}
