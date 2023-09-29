package main

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/config"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func initEnv() (*viper.Viper, error) {
	v := viper.New()

	// Configure viper to read env variables with the CLI_ prefix
	v.AutomaticEnv()
	v.SetEnvPrefix("cli")
	// Use a replacer to replace env variables underscores with points. This let us
	// use nested configurations in the config file and at the same time define
	// env variables for the nested configurations
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Add env variables supported
	v.BindEnv("id")
	v.BindEnv("log", "level")
	v.BindEnv("rabbitmq", "address")
	v.BindEnv("rabbitmq", "queue", "input")
	v.BindEnv("rabbitmq", "queue", "output")
	v.BindEnv("reducer", "columns")
	v.BindEnv("reducer", "goroutines")
	// Try to read configuration from config file. If config file
	// does not exist then ReadInConfig will fail but configuration
	// can be loaded from the environment variables, so we shouldn't
	// return an error in that case
	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Configuration could not be read from config file. Using env variables instead")
	}

	return v, nil
}

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)

	env, err := initEnv()
	if err != nil {
		log.Fatalf("%s", err)
	}

	if err := config.InitLogger(env.GetString("log.level")); err != nil {
		log.Fatalf("%s", err)
	}

	reducerConfig, err := GetConfig(env)
	if err != nil {
		log.Fatalf("%s", err)
	}

	qMiddleware := middleware.NewQueueMiddleware(reducerConfig.RabbitAddress)
	serializer := dataStructures.NewDynamicMapSerializer()
	for i := 0; i < reducerConfig.GoroutinesCount; i++ {
		r := NewReducer(i, qMiddleware, reducerConfig, serializer)
		go r.ReduceDims()
	}
	<-sigs
	qMiddleware.Close()
}
