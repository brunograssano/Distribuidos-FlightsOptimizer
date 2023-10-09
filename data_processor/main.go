package main

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

func main() {
	sigs := utils.CreateSignalListener()

	env, err := initEnv()
	if err != nil {
		log.Fatalf("%s", err)
	}

	processorConfig, err := GetConfig(env)
	if err != nil {
		log.Fatalf("%s", err)
	}

	qMiddleware := middleware.NewQueueMiddleware(processorConfig.RabbitAddress)
	serializer := dataStructures.NewSerializer()
	for i := 0; i < processorConfig.GoroutinesCount; i++ {
		r := NewDataProcessor(i, qMiddleware, processorConfig, serializer)
		go r.ProcessData()
	}
	<-sigs
	qMiddleware.Close()
}
