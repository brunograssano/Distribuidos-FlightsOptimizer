package main

import (
	"dim_reducer/reducer"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/queuefactory"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

func main() {
	sigs := utils.CreateSignalListener()

	env, err := reducer.InitEnv()
	if err != nil {
		log.Fatalf("Main - DimReducer | Error initializing env | %s", err)
	}

	reducerConfig, err := reducer.GetConfig(env)
	if err != nil {
		log.Fatalf("Main - DimReducer | Error initializing config | %s", err)
	}

	qMiddleware := middleware.NewQueueMiddleware(reducerConfig.RabbitAddress)
	queueFactory := queuefactory.NewSimpleQueueFactory(qMiddleware)
	for i := 0; i < reducerConfig.GoroutinesCount; i++ {
		r := reducer.NewReducer(i, queueFactory, reducerConfig)
		go r.ReduceDims()
	}
	<-sigs
	qMiddleware.Close()
}
