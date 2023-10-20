package main

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	queueProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

func main() {
	sigs := utils.CreateSignalListener()
	env, err := InitEnv()
	if err != nil {
		log.Fatalf("Main - Ex4 Avg Calculator | Error initializing env | %s", err)
	}
	config, err := GetConfig(env)
	if err != nil {
		log.Fatalf("Main - Ex4 Avg Calculator | Error initializing Config | %s", err)
	}

	qMiddleware := middleware.NewQueueMiddleware(config.RabbitAddress)
	inputQueue := queueProtocol.NewConsumerQueueProtocolHandler(qMiddleware.CreateConsumer(config.InputQueueName, true))
	var toJourneySavers []queueProtocol.ProducerProtocolInterface
	for i := uint(0); i < config.SaversCount; i++ {
		producer := qMiddleware.CreateExchangeProducer(config.OutputQueueName, fmt.Sprintf("%v", i), "direct", true)
		toJourneySavers = append(toJourneySavers, queueProtocol.NewProducerQueueProtocolHandler(producer))
	}

	avgCalculator := NewAvgCalculator(toJourneySavers, inputQueue, config)
	go avgCalculator.CalculateAvgLoop()
	<-sigs
	qMiddleware.Close()
}
