package main

import (
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)
	env, err := InitEnv()
	if err != nil {
		log.Fatalf("%s", err)
	}
	serverConfig, err := GetConfig(env)
	if err != nil {
		log.Fatalf("%s", err)
	}
	server := NewServer(serverConfig)
	server.StartServerLoop()
	log.Infof("Spawned Server...")
	<-sigs
	log.Infof("Ending Server...")
	_ = server.End()
}
