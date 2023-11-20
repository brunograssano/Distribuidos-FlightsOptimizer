package main

import (
	"distance_completer/config"
	"distance_completer/controllers"
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
	for i := 0; i < config.GoroutinesCount; i++ {
		distCompleter := controllers.NewDistanceCompleter(
			i,
			simpleFactory,
			config,
		)
		go distCompleter.CompleteDistances()
	}

	airportsSaver := controllers.NewAirportSaver(
		config,
		exchangeFactory,
	)
	go airportsSaver.SaveAirports()

	endSigHB := heartbeat.StartHeartbeat(config.AddressesHealthCheckers, config.ServiceName)
	<-sigs
	endSigHB <- true
	qMiddleware.Close()
}
