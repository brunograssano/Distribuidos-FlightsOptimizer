package main

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/getters"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	"log"
)

func main() {
	sigs := utils.CreateSignalListener()

	env, err := InitEnv()
	if err != nil {
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

	getterConf := getters.NewGetterConfig(saverConfig.ID, []string{saverConfig.OutputFileName}, saverConfig.GetterAddress, saverConfig.GetterBatchLines)
	getter, err := getters.NewGetter(getterConf, canSend)
	if err != nil {
		log.Fatalf("%s", err)
	}
	go getter.ReturnResults()
	<-sigs
	qMiddleware.Close()
	getter.Close()
}
