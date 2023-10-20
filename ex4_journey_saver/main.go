package main

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
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

	for i := uint(0); i < config.InternalSaversCount; i++ {
		consumerQueue := qMiddleware.CreateConsumer(fmt.Sprintf("%v-%v", config.InputQueueName, config.RoutingKeyInput+i), true)
		err = consumerQueue.BindTo(config.InputQueueName, fmt.Sprintf("%v", config.RoutingKeyInput+i), "direct")
		inputQ := queues.NewConsumerQueueProtocolHandler(consumerQueue)
		prodToAccum := queues.NewProducerQueueProtocolHandler(qMiddleware.CreateProducer(config.OutputQueueNameAccum, true))
		prodToSink := queues.NewProducerQueueProtocolHandler(qMiddleware.CreateProducer(config.OutputQueueNameSaver, true))
		js := NewJourneySaver(inputQ, prodToAccum, prodToSink)
		go js.SavePricesForJourneys()
	}

	<-sigs
	qMiddleware.Close()

}
