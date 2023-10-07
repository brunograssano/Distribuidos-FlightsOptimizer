package main

import (
	"filters_config"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filters"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	log "github.com/sirupsen/logrus"
)

type FilterStopovers struct {
	filterId   int
	config     *filters_config.FilterConfig
	consumer   protocol.ConsumerProtocolInterface
	producers  []protocol.ProducerProtocolInterface
	prodToCons protocol.ProducerProtocolInterface
	serializer *dataStructures.Serializer
	filter     *filters.Filter
}

const MinStopovers = 3

func NewFilterStopovers(filterId int, qMiddleware *middleware.QueueMiddleware, conf *filters_config.FilterConfig) *FilterStopovers {
	inputQueue := protocol.NewConsumerQueueProtocolHandler(qMiddleware.CreateConsumer(conf.InputQueueName, true))
	prodToCons := protocol.NewProducerQueueProtocolHandler(qMiddleware.CreateProducer(conf.InputQueueName, true))
	outputQueues := make([]protocol.ProducerProtocolInterface, len(conf.OutputQueueNames))
	for i := 0; i < len(conf.OutputQueueNames); i++ {
		outputQueues[i] = protocol.NewProducerQueueProtocolHandler(qMiddleware.CreateProducer(conf.OutputQueueNames[i], true))
	}
	dynMapSerializer := dataStructures.NewSerializer()
	filter := filters.NewFilter()
	return &FilterStopovers{
		filterId:   filterId,
		config:     conf,
		consumer:   inputQueue,
		producers:  outputQueues,
		prodToCons: prodToCons,
		serializer: dynMapSerializer,
		filter:     filter,
	}
}

func (fe *FilterStopovers) FilterStopovers() {
	for {
		msg, ok := fe.consumer.Pop()
		if !ok {
			log.Infof("Closing FilterStopovers Goroutine...")
			break
		}
		if msg.TypeMessage == dataStructures.EOFFlightRows {
			log.Infof("Received EOF. Now handling...")
			err := protocol.HandleEOF(msg, fe.consumer, fe.prodToCons, fe.producers)
			if err != nil {
				log.Errorf("Error handling EOF: %v", err)
			}
			break
		} else if msg.TypeMessage == dataStructures.FlightRows {
			log.Infof("Received flight rows. Now filtering...")
			fe.handleFlightRows(msg)
		} else {
			log.Warnf("Unknonw message type received. Skipping it...")
		}
	}
}

func (fe *FilterStopovers) handleFlightRows(msg *dataStructures.Message) {
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
		log.Infof("Sending filtered rows to next nodes. Input length: %v, output length: %v", len(msg.DynMaps), len(filteredRows))
		for _, producer := range fe.producers {
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
