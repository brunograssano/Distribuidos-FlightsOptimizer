package main

import (
	"github.com/brunograssano/Distribuidos-TP1/common/getters"
	"github.com/brunograssano/Distribuidos-TP1/common/heartbeat"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/queuefactory"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	"log"
	"simple_saver/saver"
)

func main() {
	sigs := utils.CreateSignalListener()

	env, err := saver.InitEnv()
	if err != nil {
		log.Fatalf("Main - Simple Saver | Error initializing env | %s", err)
	}

	config, err := saver.GetConfig(env)
	if err != nil {
		log.Fatalf("Main - Simple Saver | Error initializing config | %s", err)
	}

	qMiddleware := middleware.NewQueueMiddleware(config.RabbitAddress)
	simpleSaver := saver.NewSimpleSaver(queuefactory.NewSimpleQueueFactory(qMiddleware), config)
	go simpleSaver.SaveData()

	getterConf := getters.NewGetterConfig(config.ID, []string{config.OutputFileName}, config.GetterAddress, config.GetterBatchLines)
	getter, err := getters.NewGetter(getterConf)
	if err != nil {
		log.Fatalf("Main - Simple Saver | Error initializing Getter | %s", err)
	}
	go getter.ReturnResults()
	endSigHB := heartbeat.StartHeartbeat(config.AddressesHealthCheckers, config.ServiceName)
	<-sigs
	endSigHB <- true
	qMiddleware.Close()
	getter.Close()
}
