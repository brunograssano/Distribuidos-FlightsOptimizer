package main

import (
	"github.com/brunograssano/Distribuidos-TP1/common/config"
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
	if err := config.InitLogger(env.GetString("log.level")); err != nil {
		log.Fatalf("%s", err)
	}
	saverConfig, err := GetConfig(env)
	if err != nil {
		log.Fatalf("%s", err)
	}
	qMiddleware := middleware.NewQueueMiddleware(saverConfig.RabbitAddress)

	saverEx3 := NewSaverEx3(saverConfig)
	go saverEx3.StartHandler()

	<-sigs
	qMiddleware.Close()
	saverEx3.Close()
}
