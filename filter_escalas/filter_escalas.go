package main

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filters"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	log "github.com/sirupsen/logrus"
	"os"
	"sync"
)

type FilterEscalas struct {
	filterId     int
	config       *FilterEscalasConfig
	consumer     middleware.ConsumerInterface
	producers    []middleware.ProducerInterface
	serializer   *dataStructures.DynamicMapSerializer
	filter       *filters.Filter
	endLoop      bool
	endLoopMutex sync.Mutex
}

func NewFilterEscalas(filterId int, qMiddleware *middleware.QueueMiddleware, conf *FilterEscalasConfig) *FilterEscalas {
	inputQueue := qMiddleware.CreateConsumer(conf.InputQueueName, true)
	outputQueues := make([]middleware.ProducerInterface, len(conf.OutputQueueNames))
	for i := 0; i < len(conf.OutputQueueNames); i++ {
		outputQueues[i] = qMiddleware.CreateProducer(conf.OutputQueueNames[i], true)
	}
	dynMapSerializer := dataStructures.NewDynamicMapSerializer()
	filter := filters.NewFilter()
	return &FilterEscalas{
		filterId:   filterId,
		config:     conf,
		consumer:   inputQueue,
		producers:  outputQueues,
		serializer: dynMapSerializer,
		filter:     filter,
		endLoop:    false,
	}
}

func (fe *FilterEscalas) handleInterruptSignal(interruptChannel chan os.Signal) {
	<-interruptChannel
	log.Printf("[GORoutine - FilterEscalas%v] Received SIGTERM. Ending GoRoutine...", fe.config.ID)
	fe.endLoopMutex.Lock()
	defer fe.endLoopMutex.Unlock()
	fe.endLoop = true
}

func (fe *FilterEscalas) getEndLoopBoolean() bool {
	fe.endLoopMutex.Lock()
	defer fe.endLoopMutex.Unlock()
	return fe.endLoop
}

func (fe *FilterEscalas) FilterEscalas(interruptChannel chan os.Signal) {
	go fe.handleInterruptSignal(interruptChannel)
	for !fe.getEndLoopBoolean() {
		data := fe.consumer.Pop()
		dynamicMap := fe.serializer.Deserialize(data)
		passesFilter, err := fe.filter.GreaterOrEquals(dynamicMap, 3, "totalStopovers")
		if err != nil {
			log.Errorf("action: filter_stopovers | filter_id: %v | result: fail | skipping row | error: %v", fe.filterId, err)
		}
		if passesFilter {
			for _, producer := range fe.producers {
				producer.Send(data)
			}
		}
	}
}
