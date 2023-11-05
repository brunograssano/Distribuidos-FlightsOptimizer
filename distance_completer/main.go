package main

import (
	"distance_completer/config"
	"distance_completer/controllers"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

func main() {
	sigs := utils.CreateSignalListener()

	env, err := config.InitEnv()
	if err != nil {
		log.Fatalf("Main - Distance Completer | Error initializing env | %s", err)
	}

	completerConfig, err := config.GetConfig(env)
	if err != nil {
		log.Fatalf("Main - Distance Completer | Error initializing config | %s", err)
	}

	qMiddleware := middleware.NewQueueMiddleware(completerConfig.RabbitAddress)

	for i := 0; i < completerConfig.GoroutinesCount; i++ {
		distCompleter := controllers.NewDistanceCompleter(
			i,
			qMiddleware,
			completerConfig,
		)
		go distCompleter.CompleteDistances()
	}

	airportsSaver := controllers.NewAirportSaver(
		completerConfig,
		qMiddleware,
	)
	go airportsSaver.SaveAirports()

	<-sigs
	qMiddleware.Close()
}
