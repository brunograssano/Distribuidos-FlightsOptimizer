package main

import (
	"filters_config"
	"fmt"
	middleware "github.com/brunograssano/Distribuidos-TP1/common/middleware"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func initEnv() (*viper.Viper, error) {
	v := viper.New()

	v.AutomaticEnv()
	v.SetEnvPrefix("cli")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	_ = v.BindEnv("id")
	_ = v.BindEnv("log", "level")
	_ = v.BindEnv("rabbitmq", "queues", "input")
	_ = v.BindEnv("rabbitmq", "queues", "output")
	_ = v.BindEnv("filter", "goroutines")
	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Configuration could not be read from config file. Using env variables instead")
	}

	return v, nil
}

func main() {
	sigs := make(chan os.Signal, 1)
	defer close(sigs)
	signal.Notify(sigs, syscall.SIGTERM)
	env, err := initEnv()
	if err != nil {
		log.Fatalf("%s", err)
	}
	filterEscalasConfig, err := filters_config.GetConfigFilterEscalas(env)
	if err != nil {
		log.Fatalf("%s", err)
	}

	qMiddleware := middleware.NewQueueMiddleware(filterEscalasConfig.RabbitAddress)
	for i := 0; i < filterEscalasConfig.GoroutinesCount; i++ {
		fe := NewFilterEscalas(i, qMiddleware, filterEscalasConfig)
		log.Infof("Spawning GoRoutine #%v - Filter Escalas", i)
		go fe.FilterEscalas()
	}
	<-sigs
	qMiddleware.Close()
}
