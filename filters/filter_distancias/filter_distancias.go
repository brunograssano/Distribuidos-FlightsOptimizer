package main

import (
	"filters_config"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filters"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	log "github.com/sirupsen/logrus"
)

type FilterDistances struct {
	filterId   int
	config     *filters_config.FilterConfig
	consumer   middleware.ConsumerInterface
	producers  []middleware.ProducerInterface
	serializer *dataStructures.Serializer
	filter     *filters.Filter
}

func NewFilterDistances(filterId int, qMiddleware *middleware.QueueMiddleware, conf *filters_config.FilterConfig) *FilterDistances {
	inputQueue := qMiddleware.CreateConsumer(conf.InputQueueName, true)
	outputQueues := make([]middleware.ProducerInterface, len(conf.OutputQueueNames))
	for i := 0; i < len(conf.OutputQueueNames); i++ {
		outputQueues[i] = qMiddleware.CreateProducer(conf.OutputQueueNames[i], true)
	}
	dynMapSerializer := dataStructures.NewSerializer()
	filter := filters.NewFilter()
	return &FilterDistances{
		filterId:   filterId,
		config:     conf,
		consumer:   inputQueue,
		producers:  outputQueues,
		serializer: dynMapSerializer,
		filter:     filter,
	}
}

func (fd *FilterDistances) FilterDistances() {
	for {
		data, ok := fd.consumer.Pop()
		if !ok {
			log.Infof("Closing FilterStopovers Goroutine...")
			break
		}
		msgStruct := fd.serializer.DeserializeMsg(data)
		var filteredRows []*dataStructures.DynamicMap
		for _, row := range msgStruct.DynMaps {
			directDistance, errCast := row.GetAsFloat("directDistance")
			if errCast != nil {
				log.Errorf("action: filter_stopovers | filter_id: %v | result: fail | skipping row | error: %v", fd.filterId, errCast)
				continue
			}
			passesFilter, err := fd.filter.Greater(row, 4*directDistance, "totalTravelDistance")
			if err != nil {
				log.Errorf("action: filter_distances | filter_id: %v | result: fail | skipping row | error: %v", fd.filterId, err)
				continue
			}
			if passesFilter {
				filteredRows = append(filteredRows, row)
			}
		}
		if len(filteredRows) > 0 {
			for _, producer := range fd.producers {
				err := producer.Send(data)
				if err != nil {
					log.Errorf("Error trying to send message that passed filter...")
				}
			}
		}

	}
}
