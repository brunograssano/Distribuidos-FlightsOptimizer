package main

import (
	"filters_config"
	middleware "github.com/brunograssano/Distribuidos-TP1/common/middleware"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)
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
