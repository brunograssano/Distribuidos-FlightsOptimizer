package reducer

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	queueProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	log "github.com/sirupsen/logrus"
)

// Reducer Structure that reduces the dimensions of a row by removing columns
type Reducer struct {
	reducerId    int
	c            *Config
	consumer     queueProtocol.ConsumerProtocolInterface
	producer     queueProtocol.ProducerProtocolInterface
	prodToCons   queueProtocol.ProducerProtocolInterface
	checkpointer *checkpointer.CheckpointerHandler
}

// NewReducer Creates a new reducer
func NewReducer(
	reducerId int,
	consumer queueProtocol.ConsumerProtocolInterface,
	producer queueProtocol.ProducerProtocolInterface,
	prodToCons queueProtocol.ProducerProtocolInterface,
	c *Config,
	chkHandler *checkpointer.CheckpointerHandler,
) *Reducer {
	chkHandler.AddCheckpointable(consumer, reducerId)
	return &Reducer{
		reducerId:    reducerId,
		c:            c,
		consumer:     consumer,
		producer:     producer,
		prodToCons:   prodToCons,
		checkpointer: chkHandler,
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
			err := queueProtocol.HandleEOF(
				msg,
				r.prodToCons,
				[]queueProtocol.ProducerProtocolInterface{r.producer},
				fmt.Sprintf("%v-%v", r.c.ID, r.reducerId),
				r.c.TotalEofNodes,
			)
			if err != nil {
				log.Errorf("DimReducer %v | Error handling EOF: %v", r.reducerId, err)
			}
		} else if msg.TypeMessage == dataStructures.FlightRows {
			log.Debugf("DimReducer %v | Received flight rows. Now handling...", r.reducerId)
			r.handleFlightRows(msg)
		} else {
			log.Warnf("DimReducer %v | Received unknown type message. Skipping it...", r.reducerId)
		}
		err := r.checkpointer.DoCheckpoint(r.reducerId)
		if err != nil {
			log.Errorf("DimReducer #%v | Error on checkpointing | %v", r.reducerId, err)
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
	msg = dataStructures.NewMessageWithData(msg, rows)
	err := r.producer.Send(msg)
	if err != nil {
		log.Errorf("DimReducer %v | Error trying to send message to output queue", r.reducerId)
	}
}
