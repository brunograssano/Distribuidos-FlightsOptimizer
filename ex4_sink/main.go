package main

import (
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/queuefactory"
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
	qFactory := queuefactory.NewSimpleQueueFactory(qMiddleware)
	inputQueue := qFactory.CreateConsumer(config.InputQueueName)
	toSaver4 := qFactory.CreateProducer(config.OutputQueueName)

	sink := NewJourneySink(inputQueue, toSaver4, config.SaversCount)
	go sink.HandleJourneys()
	<-sigs
	qMiddleware.Close()
}
