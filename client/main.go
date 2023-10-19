package main

import (
	"client/client"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"os"
)

func handleSignals(sigs chan os.Signal, c *client.Client) {
	<-sigs
	c.Close()
}

func main() {
	sigs := utils.CreateSignalListener()

	env, err := client.InitEnv()
	if err != nil {
		log.Fatalf("Main - Client | Error initializing env | %s", err)
	}

	clientConfig, err := client.GetConfig(env)
	if err != nil {
		log.Fatalf("Main - Client | Error initializing config | %s", err)
	}
	c := client.NewClient(clientConfig)
	go handleSignals(sigs, c)
	c.StartClientLoop()
}
