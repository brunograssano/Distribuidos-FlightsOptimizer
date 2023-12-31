package main

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
	"github.com/brunograssano/Distribuidos-TP1/common/heartbeat"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/queuefactory"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"strconv"
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
	qFanoutFactory := queuefactory.NewFanoutExchangeQueueFactory(qMiddleware, config.OutputQueueNameAccum, "")
	qFanoutFactorySink := queuefactory.NewFanoutExchangeQueueFactory(qMiddleware, config.OutputQueueNameSaver, "")
	var services []*JourneySaver
	for i := uint(0); i < config.InternalSaversCount; i++ {
		qFactory := queuefactory.NewTopicFactory(qMiddleware, []string{"", strconv.Itoa(int(i + config.RoutingKeyInput))}, config.InputQueueName)
		inputQ := qFactory.CreateConsumer(fmt.Sprintf("%v-%v-%v", config.InputQueueName, config.ID, i+config.RoutingKeyInput))
		chkHandler := checkpointer.NewCheckpointerHandler()
		prodToAccum := qFanoutFactory.CreateProducer(config.OutputQueueNameAccum)
		prodToSink := qFanoutFactorySink.CreateProducer(config.OutputQueueNameSaver)
		js := NewJourneySaver(inputQ, prodToAccum, prodToSink, config.TotalSaversCount, chkHandler, i)
		chkHandler.RestoreCheckpoint()
		services = append(services, js)
	}

	for _, service := range services {
		go service.SavePricesForJourneys()
	}

	endSigHB := heartbeat.StartHeartbeat(config.AddressesHealthCheckers, config.ServiceName)
	<-sigs
	endSigHB <- true
	qMiddleware.Close()

}
