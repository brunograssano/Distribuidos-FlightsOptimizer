package main

import (
	"dim_reducer/reducer"
	"github.com/brunograssano/Distribuidos-TP1/common/heartbeat"
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

	config, err := reducer.GetConfig(env)
	if err != nil {
		log.Fatalf("Main - DimReducer | Error initializing config | %s", err)
	}

	qMiddleware := middleware.NewQueueMiddleware(config.RabbitAddress)
	simpleFactory := queuefactory.NewSimpleQueueFactory(qMiddleware)
	fanoutFactory := queuefactory.NewFanoutExchangeQueueFactory(qMiddleware, config.OutputQueueName, "")
	for i := 0; i < config.GoroutinesCount; i++ {
		consumer := simpleFactory.CreateConsumer(config.InputQueueName)
		producer := fanoutFactory.CreateProducer(config.OutputQueueName)
		prodToCons := simpleFactory.CreateProducer(config.InputQueueName)
		r := reducer.NewReducer(i, consumer, producer, prodToCons, config)
		go r.ReduceDims()
	}
	endSigHB := heartbeat.StartHeartbeat(config.AddressesHealthCheckers, config.ServiceName)
	<-sigs
	endSigHB <- true
	qMiddleware.Close()
}
