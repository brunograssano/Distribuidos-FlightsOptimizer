package main

import (
	"filters_config"
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filters"
	queueProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	"github.com/brunograssano/Distribuidos-TP1/common/queuefactory"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

type FilterDistances struct {
	filterId     int
	config       *filters_config.FilterConfig
	consumer     queueProtocol.ConsumerProtocolInterface
	producers    []queueProtocol.ProducerProtocolInterface
	prodToCons   queueProtocol.ProducerProtocolInterface
	filter       *filters.Filter
	checkpointer *checkpointer.CheckpointerHandler
}

func NewFilterDistances(
	filterId int,
	qFactory queuefactory.QueueProtocolFactory,
	conf *filters_config.FilterConfig,
	chkHandler *checkpointer.CheckpointerHandler,
) *FilterDistances {
	inputQueue := qFactory.CreateConsumer(conf.InputQueueName)
	prodToCons := qFactory.CreateProducer(conf.InputQueueName)
	outputQueues := make([]queueProtocol.ProducerProtocolInterface, len(conf.OutputQueueNames))
	for i := 0; i < len(conf.OutputQueueNames); i++ {
		outputQueues[i] = qFactory.CreateProducer(conf.OutputQueueNames[i])
	}
	chkHandler.AddCheckpointable(inputQueue, filterId)

	filter := filters.NewFilter()
	return &FilterDistances{
		filterId:     filterId,
		config:       conf,
		consumer:     inputQueue,
		prodToCons:   prodToCons,
		producers:    outputQueues,
		filter:       filter,
		checkpointer: chkHandler,
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
			err := queueProtocol.HandleEOF(msgStruct, fd.prodToCons, fd.producers, fmt.Sprintf("%v-%v", fd.config.ID, fd.filterId), fd.config.TotalEofNodes)
			if err != nil {
				log.Errorf("FilterDistances %v | Error handling EOF | %v", fd.filterId, err)
			}
		} else if msgStruct.TypeMessage == dataStructures.FlightRows {
			log.Debugf("FilterDistances %v | Received FlightRows. Filtering...", fd.filterId)
			fd.handleFlightRows(msgStruct)
		} else {
			log.Warnf("FilterDistances %v | Received unknown message type | Skipping...", fd.filterId)
		}
		err := fd.checkpointer.DoCheckpoint(fd.filterId)
		if err != nil {
			log.Errorf("FilterDistances #%v | Error on checkpointing | %v", fd.filterId, err)
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
			err := producer.Send(dataStructures.NewMessageWithData(msg, filteredRows))
			if err != nil {
				log.Errorf("FilterDistances %v | Error trying to send message that passed filter | %v", fd.filterId, err)
			}
		}
	}
}
