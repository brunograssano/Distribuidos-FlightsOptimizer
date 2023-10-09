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
	filterEscalasConfig, err := filters_config.GetConfigFilters(env)
	if err != nil {
		log.Fatalf("%s", err)
	}

	qMiddleware := middleware.NewQueueMiddleware(filterEscalasConfig.RabbitAddress)
	for i := 0; i < filterEscalasConfig.GoroutinesCount; i++ {
		fe := NewFilterStopovers(i, qMiddleware, filterEscalasConfig)
		log.Infof("Spawning GoRoutine #%v - Filter Escalas", i)
		go fe.FilterStopovers()
	}
	<-sigs
	qMiddleware.Close()
}
