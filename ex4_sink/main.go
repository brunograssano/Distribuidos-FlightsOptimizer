package main

import (
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	queueProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

func main() {
	sigs := utils.CreateSignalListener()
	env, err := InitEnv()
	if err != nil {
		log.Fatalf("Main - Ex4 Sink | Error initializing env | %s", err)
	}
	config, err := GetConfig(env)
	if err != nil {
		log.Fatalf("Main - Ex4 Sink | Error initializing Config | %s", err)
	}
	qMiddleware := middleware.NewQueueMiddleware(config.RabbitAddress)
	inputQueue := queueProtocol.NewConsumerQueueProtocolHandler(qMiddleware.CreateConsumer(config.InputQueueName, true))
	toSaver4 := queueProtocol.NewProducerQueueProtocolHandler(qMiddleware.CreateProducer(config.OutputQueueName, true))

	sink := NewJourneySink(inputQueue, toSaver4, config.SaversCount)
	go sink.HandleJourneys()
	<-sigs
	qMiddleware.Close()
}
