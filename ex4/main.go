package main

import (
	log "github.com/sirupsen/logrus"
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
