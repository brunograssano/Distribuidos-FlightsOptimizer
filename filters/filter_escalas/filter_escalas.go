package main

import (
	"filters_config"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filters"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	log "github.com/sirupsen/logrus"
)

type FilterStopovers struct {
	filterId   int
	config     *filters_config.FilterConfig
	consumer   middleware.ConsumerInterface
	producers  []middleware.ProducerInterface
	serializer *dataStructures.Serializer
	filter     *filters.Filter
}

const MinStopovers = 3

func NewFilterStopovers(filterId int, qMiddleware *middleware.QueueMiddleware, conf *filters_config.FilterConfig) *FilterStopovers {
	inputQueue := qMiddleware.CreateConsumer(conf.InputQueueName, true)
	outputQueues := make([]middleware.ProducerInterface, len(conf.OutputQueueNames))
	for i := 0; i < len(conf.OutputQueueNames); i++ {
		outputQueues[i] = qMiddleware.CreateProducer(conf.OutputQueueNames[i], true)
	}
	dynMapSerializer := dataStructures.NewSerializer()
	filter := filters.NewFilter()
	return &FilterStopovers{
		filterId:   filterId,
		config:     conf,
		consumer:   inputQueue,
		producers:  outputQueues,
		serializer: dynMapSerializer,
		filter:     filter,
	}
}

func (fe *FilterStopovers) FilterStopovers() {
	for {
		data, ok := fe.consumer.Pop()
		if !ok {
			log.Infof("Closing FilterStopovers Goroutine...")
			break
		}
		msg := fe.serializer.DeserializeMsg(data)
		var filteredRows []*dataStructures.DynamicMap
		for _, row := range msg.DynMaps {
			passesFilter, err := fe.filter.GreaterOrEquals(row, MinStopovers, "totalStopovers")
			if err != nil {
				log.Errorf("action: filter_stopovers | filter_id: %v | result: fail | skipping row | error: %v", fe.filterId, err)
			}
			if passesFilter {
				filteredRows = append(filteredRows, row)
			}
		}
		if len(filteredRows) > 0 {
			for _, producer := range fe.producers {
				err := producer.Send(data)
				if err != nil {
					log.Errorf("Error trying to send message that passed filter...")
				}
			}
		}
	}
}
