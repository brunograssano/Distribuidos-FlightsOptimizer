package main

import (
	"distance_completer/config"
	"distance_completer/controllers"
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
	"github.com/brunograssano/Distribuidos-TP1/common/heartbeat"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/queuefactory"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

func main() {
	sigs := utils.CreateSignalListener()

	env, err := config.InitEnv()
	if err != nil {
		log.Fatalf("Main - Distance Completer | Error initializing env | %s", err)
	}

	config, err := config.GetConfig(env)
	if err != nil {
		log.Fatalf("Main - Distance Completer | Error initializing config | %s", err)
	}

	qMiddleware := middleware.NewQueueMiddleware(config.RabbitAddress)
	simpleFactory := queuefactory.NewSimpleQueueFactory(qMiddleware)
	exchangeFactory := queuefactory.NewFanoutExchangeQueueFactory(qMiddleware, config.ExchangeNameAirports, config.RoutingKeyExchangeAirports)
	var services []*controllers.DistanceCompleter
	for i := 0; i < config.GoroutinesCount; i++ {
		checkpointerHandler := checkpointer.NewCheckpointerHandler()
		distCompleter := controllers.NewDistanceCompleter(
			i,
			simpleFactory,
			config,
			checkpointerHandler,
		)
		services = append(services, distCompleter)
		checkpointerHandler.RestoreCheckpoint()

	}

	for i, service := range services {
		log.Infof("Main Completer | Spawning GoRoutine - Completer #%v", i)
		go service.CompleteDistances()
	}
	checkpointerHandler := checkpointer.NewCheckpointerHandler()
	airportsSaver := controllers.NewAirportSaver(
		config,
		exchangeFactory,
		checkpointerHandler,
	)
	checkpointerHandler.RestoreCheckpoint()
	go airportsSaver.SaveAirports()

	endSigHB := heartbeat.StartHeartbeat(config.AddressesHealthCheckers, config.ServiceName)
	<-sigs
	endSigHB <- true
	qMiddleware.Close()
}
