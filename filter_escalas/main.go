package main

import (
	"encoding/binary"
	"fmt"
	middleware "github.com/brunograssano/Distribuidos-TP1/common/middleware"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func initEnv() (*viper.Viper, error) {
	v := viper.New()

	v.AutomaticEnv()
	v.SetEnvPrefix("cli")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	_ = v.BindEnv("id")
	_ = v.BindEnv("log", "level")
	_ = v.BindEnv("filter", "queues", "input")
	_ = v.BindEnv("filter", "queues", "output")
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
	filterEscalasConfig, err := GetConfigFilterEscalas(env)
	if err != nil {
		log.Fatalf("%s", err)
	}

	if filterEscalasConfig.ID == "0" {
		qMiddleware := middleware.NewQueueMiddleware()
		interruptChannels := make([]chan os.Signal, filterEscalasConfig.GoroutinesCount)
		for i := 0; i < filterEscalasConfig.GoroutinesCount; i++ {
			fe := NewFilterEscalas(i, qMiddleware, filterEscalasConfig)
			go fe.FilterEscalas(interruptChannels[i])
		}
		signalReceived := <-sigs
		qMiddleware.Close()
		for _, channel := range interruptChannels {
			channel <- signalReceived
			close(channel)
		}
	} else {
		qMiddleware := middleware.NewQueueMiddleware()
		prod := qMiddleware.CreateProducer(filterEscalasConfig.InputQueueName, true)
		randGen := rand.New(rand.NewSource(time.Now().UnixNano()))
		totalFilteredResult := 0

		for i := 0; i < 100; i++ {
			bytesNCols := make([]byte, 4)
			binary.BigEndian.PutUint32(bytesNCols, uint32(1))
			bytesLenKey := make([]byte, 4)
			binary.BigEndian.PutUint32(bytesLenKey, uint32(len("totalStopovers")))
			bytesKey := []byte("totalStopovers")
			bytesLenValue := make([]byte, 4)
			binary.BigEndian.PutUint32(bytesLenValue, uint32(4))
			bytesValue := make([]byte, 4)
			randomStopover := randGen.Intn(6)
			if randomStopover >= 3 {
				totalFilteredResult += 1
			}
			binary.BigEndian.PutUint32(bytesValue, uint32(randomStopover))
			var bytesSer []byte
			bytesSer = append(bytesSer, bytesNCols...)
			bytesSer = append(bytesSer, bytesLenKey...)
			bytesSer = append(bytesSer, bytesKey...)
			bytesSer = append(bytesSer, bytesLenValue...)
			bytesSer = append(bytesSer, bytesValue...)
			prod.Send(bytesSer)
		}
		log.Printf("Total Stopovers Should Be In Exit: %v", totalFilteredResult)
		qMiddleware.Close()
	}

}
