package main

import (
	config "github.com/brunograssano/Distribuidos-TP1/common/config"
	"log"
)

func main() {
	config.InitConfig()
	log.Print("Hello from a new app")
}
