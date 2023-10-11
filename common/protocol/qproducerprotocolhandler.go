package protocol

import (
	"github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
)

type ProducerProtocolInterface interface {
	Send(msg *data_structures.Message) error
	GetSentMessages() int
}

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

func (q *ProducerQueueProtocolHandler) Send(msg *data_structures.Message) error {
	bytes := serializer.SerializeMsg(msg)
	if msg.TypeMessage == data_structures.FlightRows {
		q.totalSent += len(msg.DynMaps)
	}
	return q.producer.Send(bytes)
}

func (q *ProducerQueueProtocolHandler) GetSentMessages() int {
	return q.totalSent
}
