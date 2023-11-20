package main

import (
	"filters_config"
	"github.com/brunograssano/Distribuidos-TP1/common/heartbeat"
	middleware "github.com/brunograssano/Distribuidos-TP1/common/middleware"
	queueProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
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
		inputQueue := qFactory.CreateConsumer(config.InputQueueName)
		prodToCons := qFactory.CreateProducer(config.InputQueueName)
		outputQueues := make([]queueProtocol.ProducerProtocolInterface, len(config.OutputQueueNames)+len(config.OutputExchangeNames))
		for i := 0; i < len(config.OutputQueueNames); i++ {
			outputQueues[i] = qFactory.CreateProducer(config.OutputQueueNames[i])
		}
		for i := 0; i < len(config.OutputExchangeNames); i++ {
			qTopicFactory := queuefactory.NewTopicFactory(qMiddleware, []string{""}, config.OutputExchangeNames[i])
			outputQueues[len(config.OutputQueueNames)+i] = qTopicFactory.CreateProducer("")
		}
		fe := NewFilterStopovers(i, inputQueue, outputQueues, prodToCons, config)
		log.Infof("Main - Filter Stopovers | Spawning GoRoutine - Filter #%v", i)
		go fe.FilterStopovers()
	}
	endSigHB := heartbeat.StartHeartbeat(config.AddressesHealthCheckers, config.ServiceName)
	<-sigs
	endSigHB <- true
	qMiddleware.Close()
}
