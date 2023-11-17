package reducer

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	queueProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	"github.com/brunograssano/Distribuidos-TP1/common/queuefactory"
	log "github.com/sirupsen/logrus"
)

// Reducer Structure that reduces the dimensions of a row by removing columns
type Reducer struct {
	reducerId  int
	c          *Config
	consumer   queueProtocol.ConsumerProtocolInterface
	producer   queueProtocol.ProducerProtocolInterface
	prodToCons queueProtocol.ProducerProtocolInterface
}

// NewReducer Creates a new reducer
func NewReducer(reducerId int, queueFactory queuefactory.QueueProtocolFactory, c *Config) *Reducer {
	consumer := queueFactory.CreateConsumer(c.InputQueueName)
	producer := queueFactory.CreateProducer(c.OutputQueueName)
	prodToCons := queueFactory.CreateProducer(c.InputQueueName)
	return &Reducer{
		reducerId:  reducerId,
		c:          c,
		consumer:   consumer,
		producer:   producer,
		prodToCons: prodToCons,
	}
}

// ReduceDims Loop that waits for input from the queue, reduces the rows by removing columns
// and sends the result to the next step
func (r *Reducer) ReduceDims() {
	log.Infof("DimReducer %v | Started goroutine", r.reducerId)
	for {
		msg, ok := r.consumer.Pop()
		if !ok {
			log.Infof("DimReducer %v | Closing goroutine...", r.reducerId)
			return
		}
		if msg.TypeMessage == dataStructures.EOFFlightRows {
			log.Infof("DimReducer %v | Received EOF. Now handling...", r.reducerId)
			err := queueProtocol.HandleEOF(msg, r.consumer, r.prodToCons, []queueProtocol.ProducerProtocolInterface{r.producer})
			if err != nil {
				log.Errorf("DimReducer %v | Error handling EOF: %v", r.reducerId, err)
			}
		} else if msg.TypeMessage == dataStructures.FlightRows {
			log.Debugf("DimReducer %v | Received flight rows. Now handling...", r.reducerId)
			r.handleFlightRows(msg)
		} else {
			log.Warnf("DimReducer %v | Received unknown type message. Skipping it...", r.reducerId)
		}
	}
}

func (r *Reducer) handleFlightRows(msg *dataStructures.Message) {
	var rows []*dataStructures.DynamicMap
	for _, row := range msg.DynMaps {
		reducedData, err := row.ReduceToColumns(r.c.ColumnsToKeep)
		if err != nil {
			log.Errorf("DimReducer %v | Error reducing column, skipping row | error: %v", r.reducerId, err)
			continue
		}
		rows = append(rows, reducedData)
	}
	msg = &dataStructures.Message{TypeMessage: dataStructures.FlightRows, DynMaps: rows, ClientId: msg.ClientId}
	err := r.producer.Send(msg)
	if err != nil {
		log.Errorf("DimReducer %v | Error trying to send message to output queue", r.reducerId)
	}
}
