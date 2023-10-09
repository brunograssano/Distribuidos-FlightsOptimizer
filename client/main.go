package main

import (
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"os"
)

func handleSignals(sigs chan os.Signal, c *Client) {
	<-sigs
	c.Close()
}

func main() {
	sigs := utils.CreateSignalListener()

	env, err := InitEnv()
	if err != nil {
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
