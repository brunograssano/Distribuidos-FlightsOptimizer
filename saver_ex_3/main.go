package main

import (
	"github.com/brunograssano/Distribuidos-TP1/common/config"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/getters"
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
	serializer := dataStructures.NewSerializer()
	canSend := make(chan bool)
	saver := NewSimpleSaver(qMiddleware, saverConfig, serializer, canSend)
	go saver.SaveData()

	getterConf := getters.NewGetterConfig(saverConfig.ID, saverConfig.OutputFileNames, saverConfig.GetterAddress, saverConfig.GetterBatchLines)
	getter, err := getters.NewGetter(getterConf, canSend)
	if err != nil {
		log.Fatalf("%s", err)
	}
	go getter.ReturnResults()
	<-sigs
	qMiddleware.Close()
	getter.Close()
}
