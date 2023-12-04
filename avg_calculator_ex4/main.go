package main

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
	"github.com/brunograssano/Distribuidos-TP1/common/heartbeat"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	queueProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	"github.com/brunograssano/Distribuidos-TP1/common/queuefactory"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"strconv"
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

	var toJourneySavers []queueProtocol.ProducerProtocolInterface
	qMiddleware := middleware.NewQueueMiddleware(config.RabbitAddress)
	qTopicFactory := queuefactory.NewTopicFactory(qMiddleware, []string{""}, config.OutputQueueName)
	qFanoutFactory := queuefactory.NewFanoutExchangeQueueFactory(qMiddleware, config.InputQueueName, "")
	inputQueue := qFanoutFactory.CreateConsumer(fmt.Sprintf("%v-%v", config.InputQueueName, config.ID))
	chkHandler := checkpointer.NewCheckpointerHandler()
	for i := uint(0); i < config.SaversCount; i++ {
		producer := qTopicFactory.CreateProducer(strconv.Itoa(int(i)))
		toJourneySavers = append(toJourneySavers, producer)
	}
	avgCalculator := NewAvgCalculator(toJourneySavers, inputQueue, config, chkHandler)
	chkHandler.RestoreCheckpoint()
	go avgCalculator.CalculateAvgLoop()
	endSigHB := heartbeat.StartHeartbeat(config.AddressesHealthCheckers, config.ServiceName)
	<-sigs
	endSigHB <- true
	qMiddleware.Close()
}
