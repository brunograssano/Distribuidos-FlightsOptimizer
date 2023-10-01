package main

import (
	"github.com/brunograssano/Distribuidos-TP1/common/config"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)

	env, err := InitEnv()
	if err != nil {
		log.Fatalf("%s", err)
	}

	if err := config.InitLogger(env.GetString("log.level")); err != nil {
		log.Fatalf("%s", err)
	}

	saverConfig, err := GetConfig(env)
	if err != nil {
		log.Fatalf("%s", err)
	}

	qMiddleware := middleware.NewQueueMiddleware(saverConfig.RabbitAddress)
	serializer := dataStructures.NewDynamicMapSerializer()
	saver := NewSimpleSaver(qMiddleware, saverConfig, serializer)
	go saver.SaveData()

	getter, err := NewGetter(saverConfig)
	if err != nil {
		log.Fatalf("%s", err)
	}
	go getter.ReturnResults()
	<-sigs
	qMiddleware.Close()
}
