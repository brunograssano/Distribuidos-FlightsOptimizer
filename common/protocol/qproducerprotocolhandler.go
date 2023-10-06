package protocol

import (
	"github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
)

type ProducerProtocolInterface interface {
	Send(msg *data_structures.Message) error
	GetSentMessages() int
}

type ProducerQueueProtocolHandler struct {
	producer   middleware.ProducerInterface
	serializer *data_structures.Serializer
	totalSent  int
}

func NewProducerQueueProtocolHandler(producer middleware.ProducerInterface) *ProducerQueueProtocolHandler {
	return &ProducerQueueProtocolHandler{
		producer:   producer,
		serializer: data_structures.NewSerializer(),
		totalSent:  0,
	}
}

func (q *ProducerQueueProtocolHandler) Send(msg *data_structures.Message) error {
	bytes := q.serializer.SerializeMsg(msg)
	q.totalSent++
	return q.producer.Send(bytes)
}

func (q *ProducerQueueProtocolHandler) GetSentMessages() int {
	return q.totalSent
}
