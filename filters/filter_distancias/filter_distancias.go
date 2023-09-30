package main

import (
	"filters_config"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filters"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	log "github.com/sirupsen/logrus"
)

type FilterDistancias struct {
	filterId   int
	config     *filters_config.FilterConfig
	consumer   middleware.ConsumerInterface
	producers  []middleware.ProducerInterface
	serializer *dataStructures.DynamicMapSerializer
	filter     *filters.Filter
}

func NewFilterDistancias(filterId int, qMiddleware *middleware.QueueMiddleware, conf *filters_config.FilterConfig) *FilterDistancias {
	inputQueue := qMiddleware.CreateConsumer(conf.InputQueueName, true)
	outputQueues := make([]middleware.ProducerInterface, len(conf.OutputQueueNames))
	for i := 0; i < len(conf.OutputQueueNames); i++ {
		outputQueues[i] = qMiddleware.CreateProducer(conf.OutputQueueNames[i], true)
	}
	dynMapSerializer := dataStructures.NewDynamicMapSerializer()
	filter := filters.NewFilter()
	return &FilterDistancias{
		filterId:   filterId,
		config:     conf,
		consumer:   inputQueue,
		producers:  outputQueues,
		serializer: dynMapSerializer,
		filter:     filter,
	}
}

func (fd *FilterDistancias) FilterDistances() {
	for {
		data, ok := fd.consumer.Pop()
		if !ok {
			log.Infof("Closing FilterEscalas Goroutine...")
			break
		}
		dynamicMap := fd.serializer.Deserialize(data)
		directDistance, errCast := dynamicMap.GetAsFloat("directDistance")
		if errCast != nil {
			log.Errorf("action: filter_stopovers | filter_id: %v | result: fail | skipping row | error: %v", fd.filterId, errCast)
			continue
		}
		passesFilter, err := fd.filter.Greater(dynamicMap, 4*directDistance, "totalTravelDistance")
		if err != nil {
			log.Errorf("action: filter_distances | filter_id: %v | result: fail | skipping row | error: %v", fd.filterId, err)
			continue
		}
		if passesFilter {
			for _, producer := range fd.producers {
				producer.Send(data)
			}
		}
	}
}
