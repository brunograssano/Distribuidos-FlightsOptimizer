package main

import (
	"filters_config"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filters"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	log "github.com/sirupsen/logrus"
)

type FilterEscalas struct {
	filterId   int
	config     *filters_config.FilterConfig
	consumer   middleware.ConsumerInterface
	producers  []middleware.ProducerInterface
	serializer *dataStructures.DynamicMapSerializer
	filter     *filters.Filter
}

const MinStopovers = 3

func NewFilterEscalas(filterId int, qMiddleware *middleware.QueueMiddleware, conf *filters_config.FilterConfig) *FilterEscalas {
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
	}
}

func (fe *FilterEscalas) FilterEscalas() {
	for {
		data, ok := fe.consumer.Pop()
		if !ok {
			log.Infof("Closing FilterEscalas Goroutine...")
			break
		}
		dynamicMap := fe.serializer.Deserialize(data)
		passesFilter, err := fe.filter.GreaterOrEquals(dynamicMap, MinStopovers, "totalStopovers")
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
