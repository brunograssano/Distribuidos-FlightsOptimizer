package main

import (
	config2 "distance_completer/config"
	"distance_completer/controllers"
	"github.com/brunograssano/Distribuidos-TP1/common/config"
	"github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)

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
			data_structures.NewDynamicMapSerializer(),
			startProcessing,
		)
		go distCompleter.CompleteDistances()
	}

	// TODO saver goroutine

	<-sigs
	qMiddleware.Close()
}
