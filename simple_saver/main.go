package main

import (
	"github.com/brunograssano/Distribuidos-TP1/common/getters"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	"log"
)

func main() {
	sigs := utils.CreateSignalListener()

	env, err := InitEnv()
	if err != nil {
		log.Fatalf("Main - Simple Saver | Error initializing env | %s", err)
	}

	saverConfig, err := GetConfig(env)
	if err != nil {
		log.Fatalf("Main - Simple Saver | Error initializing config | %s", err)
	}

	qMiddleware := middleware.NewQueueMiddleware(saverConfig.RabbitAddress)
	canSend := make(chan string, 1)
	saver := NewSimpleSaver(qMiddleware, saverConfig, canSend)
	go saver.SaveData()

	getterConf := getters.NewGetterConfig(saverConfig.ID, []string{saverConfig.OutputFileName}, saverConfig.GetterAddress, saverConfig.GetterBatchLines)
	getter, err := getters.NewGetter(getterConf, canSend)
	if err != nil {
		log.Fatalf("Main - Simple Saver | Error initializing Getter | %s", err)
	}
	go getter.ReturnResults()
	<-sigs
	qMiddleware.Close()
	getter.Close()
}
