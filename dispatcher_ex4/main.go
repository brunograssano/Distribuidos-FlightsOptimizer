package main

import (
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

func main() {
	sigs := utils.CreateSignalListener()
	env, err := InitEnv()
	if err != nil {
		log.Fatalf("Main - DispatcherEx4 | Error initializing env | %s", err)
	}
	dispatcherConfig, err := GetConfig(env)
	if err != nil {
		log.Fatalf("Main - DispatcherEx4 | Error initializing Config | %s", err)
	}
	dispatcherEx4 := NewDispatcherEx4(dispatcherConfig)
	log.Infof("Main - DispatcherEx4 | Spawned DispatcherEx4")
	go dispatcherEx4.StartDispatch()
	<-sigs
	log.Infof("Main - DispatcherEx4 | Ending DispatcherEx4")
	dispatcherEx4.Close()

}
