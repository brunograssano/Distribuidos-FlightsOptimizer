package main

import (
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

func main() {
	sigs := utils.CreateSignalListener()
	env, err := InitEnv()
	if err != nil {
		log.Fatalf("%s", err)
	}
	ex4Config, err := GetConfig(env)
	if err != nil {
		log.Fatalf("%s", err)
	}
	ex4Handler := NewEx4Handler(ex4Config)
	log.Infof("Spawned EX4 Handler...")
	ex4Handler.StartHandler()
	<-sigs
	log.Infof("Ending EX4 Handler...")
	ex4Handler.Close()

}
