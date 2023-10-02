package main

import (
	"github.com/brunograssano/Distribuidos-TP1/common/config"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
)

func handleSignals(sigs chan os.Signal, c *Client) {
	<-sigs
	c.Close()
}

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)

	env, err := InitEnv()
	if err != nil {
		log.Fatalf("%s", err)
	}

	if err := config.InitLogger(env.GetString("log.level")); err != nil {
		log.Fatalf("%s", err)
	}

	clientConfig, err := GetConfig(env)
	if err != nil {
		log.Fatalf("%s", err)
	}
	client := NewClient(clientConfig)
	go handleSignals(sigs, client)
	client.StartClientLoop()
}
