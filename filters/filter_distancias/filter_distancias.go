package main

import (
	"filters_config"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filters"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	queueProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

type FilterDistances struct {
	filterId   int
	config     *filters_config.FilterConfig
	consumer   queueProtocol.ConsumerProtocolInterface
	producers  []queueProtocol.ProducerProtocolInterface
	prodToCons queueProtocol.ProducerProtocolInterface
	filter     *filters.Filter
}

func NewFilterDistances(filterId int, qMiddleware *middleware.QueueMiddleware, conf *filters_config.FilterConfig) *FilterDistances {
	inputQueue := queueProtocol.NewConsumerQueueProtocolHandler(qMiddleware.CreateConsumer(conf.InputQueueName, true))
	prodToCons := queueProtocol.NewProducerQueueProtocolHandler(qMiddleware.CreateProducer(conf.InputQueueName, true))
	outputQueues := make([]queueProtocol.ProducerProtocolInterface, len(conf.OutputQueueNames))
	for i := 0; i < len(conf.OutputQueueNames); i++ {
		outputQueues[i] = queueProtocol.NewProducerQueueProtocolHandler(qMiddleware.CreateProducer(conf.OutputQueueNames[i], true))
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
			log.Infof("FilterDistances %v | Closing Goroutine...", fd.filterId)
			break
		}
		if msgStruct.TypeMessage == dataStructures.EOFFlightRows {
			log.Infof("FilterDistances %v | Received EOF. Handling...", fd.filterId)
			err := queueProtocol.HandleEOF(msgStruct, fd.consumer, fd.prodToCons, fd.producers)
			if err != nil {
				log.Errorf("FilterDistances %v | Error handling EOF | %v", fd.filterId, err)
			}
		} else if msgStruct.TypeMessage == dataStructures.FlightRows {
			log.Debugf("FilterDistances %v | Received FlightRows. Filtering...", fd.filterId)
			fd.handleFlightRows(msgStruct)
		} else {
			log.Warnf("FilterDistances %v | Received unknown message type | Skipping...", fd.filterId)
		}
	}
}

func (fd *FilterDistances) handleFlightRows(msg *dataStructures.Message) {
	var filteredRows []*dataStructures.DynamicMap
	for _, row := range msg.DynMaps {
		directDistance, errCast := row.GetAsFloat(utils.DirectDistance)
		if errCast != nil {
			log.Errorf("FilterDistances %v | action: filter_distances | result: fail | skipping row | error: %v", fd.filterId, errCast)
			continue
		}
		passesFilter, err := fd.filter.Greater(row, 4*directDistance, utils.TotalTravelDistance)
		if err != nil {
			log.Errorf("FilterDistances %v | action: filter_distances | result: fail | skipping row | error: %v", fd.filterId, err)
			continue
		}
		if passesFilter {
			filteredRows = append(filteredRows, row)
		}
	}
	if len(filteredRows) > 0 {
		log.Debugf("FilterDistances %v | Sending filtered rows to next nodes | Input length: %v | Output length: %v", fd.filterId, len(msg.DynMaps), len(filteredRows))
		for _, producer := range fd.producers {
			err := producer.Send(&dataStructures.Message{
				TypeMessage: dataStructures.FlightRows,
				DynMaps:     filteredRows,
				ClientId:    msg.ClientId,
			})
			if err != nil {
				log.Errorf("FilterDistances %v | Error trying to send message that passed filter | %v", fd.filterId, err)
			}
		}
	}
}
