package main

import (
	"filters_config"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filters"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	log "github.com/sirupsen/logrus"
)

type FilterDistances struct {
	filterId   int
	config     *filters_config.FilterConfig
	consumer   protocol.ConsumerProtocolInterface
	producers  []protocol.ProducerProtocolInterface
	prodToCons protocol.ProducerProtocolInterface
	filter     *filters.Filter
}

func NewFilterDistances(filterId int, qMiddleware *middleware.QueueMiddleware, conf *filters_config.FilterConfig) *FilterDistances {
	inputQueue := protocol.NewConsumerQueueProtocolHandler(qMiddleware.CreateConsumer(conf.InputQueueName, true))
	prodToCons := protocol.NewProducerQueueProtocolHandler(qMiddleware.CreateProducer(conf.InputQueueName, true))
	outputQueues := make([]protocol.ProducerProtocolInterface, len(conf.OutputQueueNames))
	for i := 0; i < len(conf.OutputQueueNames); i++ {
		outputQueues[i] = protocol.NewProducerQueueProtocolHandler(qMiddleware.CreateProducer(conf.OutputQueueNames[i], true))
	}

	filter := filters.NewFilter()
	return &FilterDistances{
		filterId:   filterId,
		config:     conf,
		consumer:   inputQueue,
		prodToCons: prodToCons,
		producers:  outputQueues,
		filter:     filter,
	}
}

func (fd *FilterDistances) FilterDistances() {
	for {
		msgStruct, ok := fd.consumer.Pop()
		if !ok {
			log.Infof("Closing FilterStopovers Goroutine...")
			break
		}
		if msgStruct.TypeMessage == dataStructures.EOFFlightRows {
			log.Infof("Received EOF. Handling...")
			err := protocol.HandleEOF(msgStruct, fd.consumer, fd.prodToCons, fd.producers)
			if err != nil {
				log.Errorf("Error handling EOF: %v", err)
			}
			break
		} else if msgStruct.TypeMessage == dataStructures.FlightRows {
			log.Infof("Received FlightRows. Filtering...")
			fd.handleFlightRows(msgStruct)
		} else {
			log.Warnf("Received unknown message type. Skipping...")
		}
	}
}

func (fd *FilterDistances) handleFlightRows(msgStruct *dataStructures.Message) {
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
		log.Infof("Sending filtered rows to next nodes. Input length: %v, output length: %v", len(msgStruct.DynMaps), len(filteredRows))
		for _, producer := range fd.producers {
			err := producer.Send(&dataStructures.Message{
				TypeMessage: dataStructures.FlightRows,
				DynMaps:     filteredRows,
			})
			if err != nil {
				log.Errorf("Error trying to send message that passed filter...")
			}
		}
	}
}
