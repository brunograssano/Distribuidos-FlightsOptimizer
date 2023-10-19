package queues

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
)

type ProducerQueueProtocolHandler struct {
	producer  middleware.ProducerInterface
	totalSent int
}

func NewProducerQueueProtocolHandler(producer middleware.ProducerInterface) *ProducerQueueProtocolHandler {
	return &ProducerQueueProtocolHandler{
		producer:  producer,
		totalSent: 0,
	}
}

func (q *ProducerQueueProtocolHandler) Send(msg *dataStructures.Message) error {
	bytes := serializer.SerializeMsg(msg)
	if msg.TypeMessage == dataStructures.FlightRows {
		q.totalSent += len(msg.DynMaps)
	}
	return q.producer.Send(bytes)
}

func (q *ProducerQueueProtocolHandler) GetSentMessages() int {
	return q.totalSent
}

func (q *ProducerQueueProtocolHandler) ClearData() {
	q.totalSent = 0
}
