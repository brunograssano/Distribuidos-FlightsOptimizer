package main

import (
	"filters_config"
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filters"
	queueProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

type FilterStopovers struct {
	filterId     int
	config       *filters_config.FilterConfig
	consumer     queueProtocol.ConsumerProtocolInterface
	producers    []queueProtocol.ProducerProtocolInterface
	prodToCons   queueProtocol.ProducerProtocolInterface
	filter       *filters.Filter
	checkpointer *checkpointer.CheckpointerHandler
}

const MinStopovers = 3

func NewFilterStopovers(
	filterId int,
	consumer queueProtocol.ConsumerProtocolInterface,
	producers []queueProtocol.ProducerProtocolInterface,
	prodToCons queueProtocol.ProducerProtocolInterface,
	conf *filters_config.FilterConfig,
	chkHandler *checkpointer.CheckpointerHandler,
) *FilterStopovers {
	filter := filters.NewFilter()
	chkHandler.AddCheckpointable(consumer, filterId)
	return &FilterStopovers{
		filterId:     filterId,
		config:       conf,
		consumer:     consumer,
		producers:    producers,
		prodToCons:   prodToCons,
		filter:       filter,
		checkpointer: chkHandler,
	}
}

func (fe *FilterStopovers) FilterStopovers() {
	for {
		msg, ok := fe.consumer.Pop()
		if !ok {
			log.Infof("FilterStopovers %v | Closing FilterStopovers Goroutine...", fe.filterId)
			break
		}
		if msg.TypeMessage == dataStructures.EOFFlightRows {
			log.Infof("FilterStopovers %v | Received EOF. Now handling...", fe.filterId)
			err := queueProtocol.HandleEOF(msg, fe.prodToCons, fe.producers, fmt.Sprintf("%v-%v", fe.config.ID, fe.filterId), fe.config.TotalEofNodes)
			if err != nil {
				log.Errorf("FilterStopovers %v | Error handling EOF | %v", fe.filterId, err)
			}
		} else if msg.TypeMessage == dataStructures.FlightRows {
			log.Debugf("FilterStopovers %v | Received flight rows. Now filtering...", fe.filterId)
			fe.handleFlightRows(msg)
		} else {
			log.Warnf("FilterStopovers %v | Warn Message | Unknonw message type received. Skipping it...", fe.filterId)
		}
		err := fe.checkpointer.DoCheckpoint(fe.filterId)
		if err != nil {
			log.Errorf("FilterStopovers #%v | Error on checkpointing | %v", fe.filterId, err)
		}
	}
}

func (fe *FilterStopovers) handleFlightRows(msg *dataStructures.Message) {
	var filteredRows []*dataStructures.DynamicMap
	for _, row := range msg.DynMaps {
		passesFilter, err := fe.filter.GreaterOrEquals(row, MinStopovers, utils.TotalStopovers)
		if err != nil {
			log.Errorf("FilterStopovers %v | action: filter_stopovers | result: fail | skipping row | error: %v", fe.filterId, err)
		}
		if passesFilter {
			filteredRows = append(filteredRows, row)
		}
	}
	if len(filteredRows) > 0 {
		log.Debugf("FilterStopovers %v | Sending filtered rows to next nodes. Input length: %v, output length: %v", fe.filterId, len(msg.DynMaps), len(filteredRows))
		for _, producer := range fe.producers {
			err := producer.Send(dataStructures.NewMessageWithData(msg, filteredRows))
			if err != nil {
				log.Errorf("FilterStopovers %v | Error trying to send message that passed filter...", fe.filterId)
			}
		}
	}
}
