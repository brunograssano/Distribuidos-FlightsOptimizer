package main

import (
	"errors"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	log "github.com/sirupsen/logrus"
	"strings"
)

const segmentSplitter = "||"
const destinationAirport = 1

type DataProcessor struct {
	processorId    int
	c              *ProcessorConfig
	consumer       middleware.ConsumerInterface
	producersEx123 []middleware.ProducerInterface
	producersEx4   middleware.ProducerInterface
	serializer     *dataStructures.DynamicMapSerializer
	ex123Columns   []string
	ex4Columns     []string
}

func NewDataProcessor(id int, qMiddleware *middleware.QueueMiddleware, c *ProcessorConfig, serializer *dataStructures.DynamicMapSerializer) *DataProcessor {
	consumer := qMiddleware.CreateConsumer(c.InputQueueName, true)
	producersEx123 := []middleware.ProducerInterface{}
	for _, queueName := range c.OutputQueueNameEx123 {
		producersEx123 = append(producersEx123, qMiddleware.CreateProducer(queueName, true))
	}
	producersEx4 := qMiddleware.CreateProducer(c.OutputQueueNameEx4, true)
	return &DataProcessor{
		processorId:    id,
		c:              c,
		consumer:       consumer,
		producersEx123: producersEx123,
		producersEx4:   producersEx4,
		serializer:     serializer,
		ex123Columns:   []string{"legId", "startingAirport", "destinationAirport", "travelDuration", "totalFare", "totalTravelDistance", "segmentsAirlineName", "totalStopovers", "route"},
		ex4Columns:     []string{"startingAirport", "destinationAirport", "totalFare"},
	}
}

func (d *DataProcessor) ProcessData() {
	for {
		msg, ok := d.consumer.Pop()
		if !ok {
			log.Infof("Closing goroutine %v", d.processorId)
			return
		}
		cols := d.serializer.Deserialize(msg)
		cols, err := d.processEx123Row(cols)
		if err != nil {
			log.Errorf("action: reduce_columns_ex123 | processor_id: %v | result: fail | skipping row | error: %v", d.processorId, err)
			continue
		}
		serialized := d.serializer.Serialize(cols)
		for _, producer := range d.producersEx123 {
			producer.Send(serialized)
		}
		cols, err = d.processEx4Row(cols)
		if err != nil {
			log.Errorf("action: reduce_columns_ex4 | processor_id: %v | result: fail | skipping row | error: %v", d.processorId, err)
			continue
		}
		serialized = d.serializer.Serialize(cols)
		d.producersEx4.Send(serialized)

	}
}

func (d *DataProcessor) processEx123Row(cols *dataStructures.DynamicMap) (*dataStructures.DynamicMap, error) {
	segments, err := cols.GetAsString("segmentsArrivalAirportCode")
	if err != nil {
		return nil, err
	}
	startingAirport, err := cols.GetAsString("startingAirport")
	if err != nil {
		return nil, err
	}
	splittedSegments := strings.Split(segments, segmentSplitter)
	splittedSegmentsLen := len(splittedSegments)
	if splittedSegmentsLen == 0 {
		return nil, errors.New("empty segment")
	}
	totalStopovers := uint32(splittedSegmentsLen) - destinationAirport
	cols.AddColumn("totalStopovers", d.serializer.SerializeUint(totalStopovers))
	route := startingAirport + segmentSplitter + segments
	cols.AddColumn("route", d.serializer.SerializeString(route))
	return cols.ReduceToColumns(d.ex123Columns)
}

func (d *DataProcessor) processEx4Row(cols *dataStructures.DynamicMap) (*dataStructures.DynamicMap, error) {
	return cols.ReduceToColumns(d.ex4Columns)
}
