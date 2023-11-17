package main

import (
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
		log.Fatalf("Main - Ex4 Journey Saver | Error initializing env | %s", err)
	}
	config, err := GetConfig(env)
	if err != nil {
		log.Fatalf("Main - Ex4 Journey Saver | Error initializing Config | %s", err)
	}
	qMiddleware := middleware.NewQueueMiddleware(config.RabbitAddress)
	qFactory := queuefactory.NewDirectExchangeConsumerSimpleProdQueueFactory(qMiddleware, config.RoutingKeyInput)
	for i := uint(0); i < config.InternalSaversCount; i++ {
		inputQ := qFactory.CreateConsumer(config.InputQueueName)
		prodToAccum := qFactory.CreateProducer(config.OutputQueueNameAccum)
		prodToSink := qFactory.CreateProducer(config.OutputQueueNameSaver)
		js := NewJourneySaver(inputQ, prodToAccum, prodToSink)
		go js.SavePricesForJourneys()
	}

	endSigHB := heartbeat.StartHeartbeat(config.AddressesHealthCheckers, config.ServiceName)
	<-sigs
	endSigHB <- true
	qMiddleware.Close()

}
