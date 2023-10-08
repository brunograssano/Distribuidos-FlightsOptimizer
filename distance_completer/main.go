package main

import (
	config2 "distance_completer/config"
	"distance_completer/controllers"
	"github.com/brunograssano/Distribuidos-TP1/common/config"
	"github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

func main() {
	sigs := utils.CreateSignalListener()

	env, err := config2.InitEnv()
	if err != nil {
		log.Fatalf("%s", err)
	}

	if err := config.InitLogger(env.GetString("log.level")); err != nil {
		log.Fatalf("%s", err)
	}

	completerConfig, err := config2.GetConfig(env)
	if err != nil {
		log.Fatalf("%s", err)
	}

	qMiddleware := middleware.NewQueueMiddleware(completerConfig.RabbitAddress)

	var startChannels []chan bool
	for i := 0; i < completerConfig.GoroutinesCount; i++ {
		startProcessing := make(chan bool)
		startChannels = append(startChannels, startProcessing)
		distCompleter := controllers.NewDistanceCompleter(
			i,
			qMiddleware,
			completerConfig,
			data_structures.NewSerializer(),
			startProcessing,
		)
		go distCompleter.CompleteDistances()
	}

	airportsSaver := controllers.NewAirportSaver(
		completerConfig,
		qMiddleware,
		data_structures.NewSerializer(),
		startChannels,
	)
	go airportsSaver.SaveAirports()

	<-sigs
	qMiddleware.Close()
}
