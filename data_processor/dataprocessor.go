package main

import (
	"errors"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	log "github.com/sirupsen/logrus"
	"strings"
)

const segmentSplitter = "||"
const destinationAirport = 1

// DataProcessor Structure that handles the initial row preprocessing by removing columns and creating auxiliary columns
type DataProcessor struct {
	processorId    int
	c              *ProcessorConfig
	consumer       protocol.ConsumerProtocolInterface
	producersEx123 []protocol.ProducerProtocolInterface
	producersEx4   protocol.ProducerProtocolInterface
	serializer     *dataStructures.Serializer
	ex123Columns   []string
	ex4Columns     []string
	inputQueueProd protocol.ProducerProtocolInterface
}

// NewDataProcessor Creates a new DataProcessor structure
func NewDataProcessor(id int, qMiddleware *middleware.QueueMiddleware, c *ProcessorConfig, serializer *dataStructures.Serializer) *DataProcessor {
	consumer := protocol.NewConsumerQueueProtocolHandler(qMiddleware.CreateConsumer(c.InputQueueName, true))
	var producersEx123 []protocol.ProducerProtocolInterface
	for _, queueName := range c.OutputQueueNameEx123 {
		producersEx123 = append(producersEx123, protocol.NewProducerQueueProtocolHandler(qMiddleware.CreateProducer(queueName, true)))
	}
	producersEx4 := protocol.NewProducerQueueProtocolHandler(qMiddleware.CreateProducer(c.OutputQueueNameEx4, true))
	inputQProd := protocol.NewProducerQueueProtocolHandler(qMiddleware.CreateProducer(c.InputQueueName, true))
	return &DataProcessor{
		processorId:    id,
		c:              c,
		consumer:       consumer,
		producersEx123: producersEx123,
		producersEx4:   producersEx4,
		serializer:     serializer,
		ex123Columns:   []string{"legId", "startingAirport", "destinationAirport", "travelDuration", "totalFare", "totalTravelDistance", "segmentsAirlineName", "totalStopovers", "route"},
		ex4Columns:     []string{"startingAirport", "destinationAirport", "totalFare"},
		inputQueueProd: inputQProd,
	}
}

func (d *DataProcessor) processRows(rows []*dataStructures.DynamicMap) ([]*dataStructures.DynamicMap, []*dataStructures.DynamicMap) {

	var ex123Rows []*dataStructures.DynamicMap
	var ex4Rows []*dataStructures.DynamicMap
	for _, cols := range rows {
		cols, err := d.processEx123Row(cols)
		if err != nil {
			log.Errorf("action: reduce_columns_ex123 | processor_id: %v | result: fail | skipping row | error: %v", d.processorId, err)
			continue
		}
		ex123Rows = append(ex123Rows, cols)
		cols, err = d.processEx4Row(cols)
		if err != nil {
			log.Errorf("action: reduce_columns_ex4 | processor_id: %v | result: fail | skipping row | error: %v", d.processorId, err)
			continue
		}
		ex4Rows = append(ex4Rows, cols)
	}
	return ex123Rows, ex4Rows
}

// ProcessData General loop that listens to the queue, preprocess the data, and passes it to the next steps
func (d *DataProcessor) ProcessData() {
	defer log.Infof("Closing goroutine %v", d.processorId)
	for {
		msg, ok := d.consumer.Pop()
		if !ok {
			return
		}
		if msg.TypeMessage == dataStructures.EOFFlightRows {
			log.Infof("Received EOF from server. Now finishing...")
			_ = protocol.HandleEOF(msg, d.consumer, d.inputQueueProd, append(d.producersEx123, d.producersEx4))
			return
		} else if msg.TypeMessage == dataStructures.FlightRows {
			log.Infof("Received Batch of Rows. Now processing...\n")
			ex123Rows, ex4Rows := d.processRows(msg.DynMaps)
			log.Infof("Sending processed rows to next nodes...\n")
			d.sendToEx123(ex123Rows)
			d.sendToEx4(ex4Rows)
		} else {
			log.Warnf("Received unknown type of message. Skipping it...")
		}
	}
}

func (d *DataProcessor) sendToEx4(ex4Rows []*dataStructures.DynamicMap) {
	msg := &dataStructures.Message{TypeMessage: dataStructures.FlightRows, DynMaps: ex4Rows}
	err := d.producersEx4.Send(msg)
	if err != nil {
		log.Errorf("Error trying to send to exercise 4 the serialized row")
	}
	log.Infof("Ending send of batch for Ex4...")
}

func (d *DataProcessor) sendToEx123(ex123Rows []*dataStructures.DynamicMap) {
	msg := &dataStructures.Message{TypeMessage: dataStructures.FlightRows, DynMaps: ex123Rows}
	for _, producer := range d.producersEx123 {
		err := producer.Send(msg)
		if err != nil {
			log.Errorf("Error trying to send to exercises 1,2,3 the serialized row")
		}
	}
	log.Infof("Ending send of batch for Ex 1,2,3...")

}

// processEx4Row Exercise 1,2 & 3 preprocessing. Removes columns, calculates total stopovers, and makes the route
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

// processEx4Row Exercise 4 preprocessing. Removes unnecessary columns
func (d *DataProcessor) processEx4Row(cols *dataStructures.DynamicMap) (*dataStructures.DynamicMap, error) {
	return cols.ReduceToColumns(d.ex4Columns)
}
