package main

import (
	"github.com/brunograssano/Distribuidos-TP1/common/config"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)

	env, err := initEnv()
	if err != nil {
		log.Fatalf("%s", err)
	}

	if err := config.InitLogger(env.GetString("log.level")); err != nil {
		log.Fatalf("%s", err)
	}

	reducerConfig, err := GetConfig(env)
	if err != nil {
		log.Fatalf("%s", err)
	}

	qMiddleware := middleware.NewQueueMiddleware(reducerConfig.RabbitAddress)
	serializer := dataStructures.NewDynamicMapSerializer()
	for i := 0; i < reducerConfig.GoroutinesCount; i++ {
		r := NewReducer(i, qMiddleware, reducerConfig, serializer)
		go r.ReduceDims()
	}
	<-sigs
	qMiddleware.Close()
}
