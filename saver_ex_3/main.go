package main

import (
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	"log"
)

func main() {
	sigs := utils.CreateSignalListener()
	env, err := InitEnv()
	if err != nil {
		log.Fatalf("Main - Saver Ex3 | Error initializing env | %s", err)
	}

	saverConfig, err := GetConfig(env)
	if err != nil {
		log.Fatalf("Main - Saver Ex3 | Error initializing config | %s", err)
	}
	qMiddleware := middleware.NewQueueMiddleware(saverConfig.RabbitAddress)

	saverEx3 := NewEx3Handler(saverConfig)
	go saverEx3.StartHandler()

	<-sigs
	qMiddleware.Close()
	saverEx3.Close()
}
