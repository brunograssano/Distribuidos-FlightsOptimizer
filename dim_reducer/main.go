package main

import (
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

func main() {
	sigs := utils.CreateSignalListener()

	env, err := initEnv()
	if err != nil {
		log.Fatalf("%s", err)
	}

	reducerConfig, err := GetConfig(env)
	if err != nil {
		log.Fatalf("%s", err)
	}

	qMiddleware := middleware.NewQueueMiddleware(reducerConfig.RabbitAddress)
	for i := 0; i < reducerConfig.GoroutinesCount; i++ {
		r := NewReducer(i, qMiddleware, reducerConfig)
		go r.ReduceDims()
	}
	<-sigs
	qMiddleware.Close()
}
