package main

import (
	"github.com/brunograssano/Distribuidos-TP1/common/getters"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
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

	saverConfig, err := saver.GetConfig(env)
	if err != nil {
		log.Fatalf("Main - Simple Saver | Error initializing config | %s", err)
	}

	qMiddleware := middleware.NewQueueMiddleware(saverConfig.RabbitAddress)
	simpleSaver := saver.NewSimpleSaver(qMiddleware, saverConfig)
	go simpleSaver.SaveData()

	getterConf := getters.NewGetterConfig(saverConfig.ID, []string{saverConfig.OutputFileName}, saverConfig.GetterAddress, saverConfig.GetterBatchLines)
	getter, err := getters.NewGetter(getterConf)
	if err != nil {
		log.Fatalf("Main - Simple Saver | Error initializing Getter | %s", err)
	}
	go getter.ReturnResults()
	<-sigs
	qMiddleware.Close()
	getter.Close()
}
