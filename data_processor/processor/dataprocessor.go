package processor

import (
	"errors"
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	queueProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	"github.com/brunograssano/Distribuidos-TP1/common/queuefactory"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"os"
	"strings"
)

const destinationAirport = 1

// DataProcessor Structure that handles the initial row preprocessing by removing columns and creating auxiliary columns
type DataProcessor struct {
	processorId    int
	c              *Config
	consumer       queueProtocol.ConsumerProtocolInterface
	producersEx123 []queueProtocol.ProducerProtocolInterface
	producersEx4   queueProtocol.ProducerProtocolInterface
	ex123Columns   []string
	ex4Columns     []string
	inputQueueProd queueProtocol.ProducerProtocolInterface
	checkpointer   *checkpointer.CheckpointerHandler
	toAllProducers []queueProtocol.ProducerProtocolInterface
}

// NewDataProcessor Creates a new DataProcessor structure
func NewDataProcessor(id int, qFactory queuefactory.QueueProtocolFactory, c *Config, chkHandler *checkpointer.CheckpointerHandler) *DataProcessor {
	consumer := qFactory.CreateConsumer(c.InputQueueName)
	var producersEx123 []queueProtocol.ProducerProtocolInterface
	var toAllProducers []queueProtocol.ProducerProtocolInterface
	for _, queueName := range c.OutputQueueNameEx123 {
		prod := qFactory.CreateProducer(queueName)
		producersEx123 = append(producersEx123, prod)
		toAllProducers = append(toAllProducers, prod)
	}
	producersEx4 := qFactory.CreateProducer(c.OutputQueueNameEx4)
	toAllProducers = append(toAllProducers, producersEx4)
	inputQProd := qFactory.CreateProducer(c.InputQueueName)
	chkHandler.AddCheckpointable(consumer, id)
	return &DataProcessor{
		processorId:    id,
		c:              c,
		consumer:       consumer,
		producersEx123: producersEx123,
		producersEx4:   producersEx4,
		ex123Columns:   []string{utils.LegId, utils.StartingAirport, utils.DestinationAirport, utils.TravelDuration, utils.TotalFare, utils.TotalTravelDistance, utils.SegmentsAirlineName, utils.TotalStopovers, utils.Route},
		ex4Columns:     []string{utils.StartingAirport, utils.DestinationAirport, utils.TotalFare},
		inputQueueProd: inputQProd,
		checkpointer:   chkHandler,
		toAllProducers: toAllProducers,
	}
}

func (d *DataProcessor) processRows(rows []*dataStructures.DynamicMap) ([]*dataStructures.DynamicMap, []*dataStructures.DynamicMap) {

	var ex123Rows []*dataStructures.DynamicMap
	var ex4Rows []*dataStructures.DynamicMap
	for _, cols := range rows {
		cols, err := d.processEx123Row(cols)
		if err != nil {
			log.Errorf("DataProcesssor %v | action: reduce_columns_ex123 | result: fail | skipping row | error: %v", d.processorId, err)
			continue
		}
		ex123Rows = append(ex123Rows, cols)
		cols, err = d.processEx4Row(cols)
		if err != nil {
			log.Errorf("DataProcessor %v | action: reduce_columns_ex4 | result: fail | skipping row | error: %v", d.processorId, err)
			continue
		}
		ex4Rows = append(ex4Rows, cols)
	}
	return ex123Rows, ex4Rows
}

// ProcessData General loop that listens to the queue, preprocess the data, and passes it to the next steps
func (d *DataProcessor) ProcessData() {
	defer log.Infof("DataProcessor %v | Closing goroutine...", d.processorId)
	counter := 0
	for {
		msg, ok := d.consumer.Pop()
		if !ok {
			return
		}
		log.Infof("DP %v | mensaje %v %v-%v-%v", d.processorId, msg.TypeMessage, msg.ClientId, msg.MessageId, msg.RowId)
		if msg.TypeMessage == dataStructures.EOFFlightRows {
			log.Infof("DataProcessor %v | Received EOF from server. Now finishing...", d.processorId)
			_ = queueProtocol.HandleEOF(
				msg,
				d.inputQueueProd,
				d.toAllProducers,
				fmt.Sprintf("%v-%v", d.c.ID, d.processorId),
				d.c.TotalEofNodes,
			)
		} else if msg.TypeMessage == dataStructures.FlightRows {
			log.Debugf("DataProcessor %v | Received Batch of Rows. Now processing...", d.processorId)
			ex123Rows, ex4Rows := d.processRows(msg.DynMaps)
			log.Debugf("DataProcessor %v | Sending processed rows to next nodes...", d.processorId)
			d.sendToEx123(ex123Rows, msg)
			d.sendToEx4(ex4Rows, msg)
		} else {
			log.Warnf("DataProcessor %v | Warning Messsage | Received unknown type of message. Skipping it...", d.processorId)
		}
		if counter > 5 {
			log.Errorf("EXIT %v", d.processorId)
			os.Exit(137)
		}
		counter++
		err := d.checkpointer.DoCheckpoint(d.processorId)
		if err != nil {
			log.Errorf("DataProcessor #%v | Error on checkpointing | %v", d.processorId, err)
		}
	}
}

func (d *DataProcessor) sendToEx4(ex4Rows []*dataStructures.DynamicMap, oldMsg *dataStructures.Message) {
	msg := dataStructures.NewMessageWithData(oldMsg, ex4Rows)
	err := d.producersEx4.Send(msg)
	if err != nil {
		log.Errorf("DataProcessor %v | Error trying to send to exercise 4 the serialized row | %v", d.processorId, err)
	}
	log.Debugf("DataProcessor %v | Ending send of batch for Ex4...", d.processorId)
}

func (d *DataProcessor) sendToEx123(ex123Rows []*dataStructures.DynamicMap, oldMsg *dataStructures.Message) {
	msg := dataStructures.NewMessageWithData(oldMsg, ex123Rows)
	for _, producer := range d.producersEx123 {
		err := producer.Send(msg)
		if err != nil {
			log.Errorf("DataProcessor %v | Error trying to send to exercises 1,2,3 the serialized row | %v", d.processorId, err)
		}
	}
	log.Debugf("DataProcessor %v | Ending send of batch for Ex 1,2,3...", d.processorId)

}

// processEx4Row Exercise 1,2 & 3 preprocessing. Removes columns, calculates total stopovers, and makes the route
func (d *DataProcessor) processEx123Row(cols *dataStructures.DynamicMap) (*dataStructures.DynamicMap, error) {
	segments, err := cols.GetAsString(utils.SegmentsArrivalAirportCode)
	if err != nil {
		return nil, err
	}
	startingAirport, err := cols.GetAsString(utils.StartingAirport)
	if err != nil {
		return nil, err
	}
	splittedSegments := strings.Split(segments, utils.DoublePipeSeparator)
	splittedSegmentsLen := len(splittedSegments)
	if splittedSegmentsLen == 0 {
		return nil, errors.New("empty segment")
	}

	totalStopovers := uint32(splittedSegmentsLen) - destinationAirport
	cols.AddColumn(utils.TotalStopovers, serializer.SerializeUint(totalStopovers))

	route := startingAirport + utils.DoublePipeSeparator + segments
	cols.AddColumn(utils.Route, serializer.SerializeString(route))
	return cols.ReduceToColumns(d.ex123Columns)
}

// processEx4Row Exercise 4 preprocessing. Removes unnecessary columns
func (d *DataProcessor) processEx4Row(cols *dataStructures.DynamicMap) (*dataStructures.DynamicMap, error) {
	return cols.ReduceToColumns(d.ex4Columns)
}
