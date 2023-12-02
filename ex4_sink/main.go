package main

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/heartbeat"
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
	qFanoutInputFactory := queuefactory.NewFanoutExchangeQueueFactory(qMiddleware, config.InputQueueName, "")
	qFanoutOutputFactory := queuefactory.NewFanoutExchangeQueueFactory(qMiddleware, config.OutputQueueName, "")
	iQueueName := fmt.Sprintf("%v-%v", config.InputQueueName, config.ID)
	inputQueue := qFanoutInputFactory.CreateConsumer(iQueueName, iQueueName)
	toSaver4 := qFanoutOutputFactory.CreateProducer(config.OutputQueueName)

	sink := NewJourneySink(inputQueue, toSaver4, config.SaversCount)
	go sink.HandleJourneys()
	endSigHB := heartbeat.StartHeartbeat(config.AddressesHealthCheckers, config.ServiceName)
	<-sigs
	endSigHB <- true
	qMiddleware.Close()
}
