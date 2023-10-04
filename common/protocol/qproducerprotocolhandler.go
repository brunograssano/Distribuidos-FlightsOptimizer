package protocol

import (
	"github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
)

type ProducerProtocolInterface interface {
	Send(msg *data_structures.Message) error
}

type ProducerQueueProtocolHandler struct {
	producer   middleware.ProducerInterface
	serializer *data_structures.Serializer
}

func NewProducerQueueProtocolHandler(producer middleware.ProducerInterface) *ProducerQueueProtocolHandler {
	return &ProducerQueueProtocolHandler{
		producer:   producer,
		serializer: data_structures.NewSerializer(),
	}
}

func (q *ProducerQueueProtocolHandler) Send(msg *data_structures.Message) error {
	bytes := q.serializer.SerializeMsg(msg)
	return q.producer.Send(bytes)
}
