package main

import (
	"filters_config"
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
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
	var services []*FilterStopovers
	for i := 0; i < config.GoroutinesCount; i++ {
		checkpointerHandler := checkpointer.NewCheckpointerHandler()
		inputQueue := qFactory.CreateConsumer(config.InputQueueName, fmt.Sprintf("%v-%v-%v", config.ID, config.InputQueueName, i))
		prodToCons := qFactory.CreateProducer(config.InputQueueName)
		outputQueues := make([]queueProtocol.ProducerProtocolInterface, len(config.OutputQueueNames)+len(config.OutputExchangeNames))
		for i := 0; i < len(config.OutputQueueNames); i++ {
			outputQueues[i] = qFactory.CreateProducer(config.OutputQueueNames[i])
		}
		for i := 0; i < len(config.OutputExchangeNames); i++ {
			qTopicFactory := queuefactory.NewTopicFactory(qMiddleware, []string{""}, config.OutputExchangeNames[i])
			outputQueues[len(config.OutputQueueNames)+i] = qTopicFactory.CreateProducer("")
		}
		fe := NewFilterStopovers(i, inputQueue, outputQueues, prodToCons, config, checkpointerHandler)
		services = append(services, fe)
		checkpointerHandler.RestoreCheckpoint()
	}
	for i := 0; i < config.GoroutinesCount; i++ {
		log.Infof("Main - Filter Stopovers | Spawning GoRoutine - Filter #%v", i)
		go services[i].FilterStopovers()
	}
	endSigHB := heartbeat.StartHeartbeat(config.AddressesHealthCheckers, config.ServiceName)
	<-sigs
	endSigHB <- true
	qMiddleware.Close()
}
