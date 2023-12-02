package queues

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
)

type ProducerQueueProtocolHandler struct {
	producer middleware.ProducerInterface
}

func NewProducerQueueProtocolHandler(producer middleware.ProducerInterface) *ProducerQueueProtocolHandler {
	return &ProducerQueueProtocolHandler{
		producer: producer,
	}
}

func (q *ProducerQueueProtocolHandler) Send(msg *dataStructures.Message) error {
	bytes := serializer.SerializeMsg(msg)
	return q.producer.Send(bytes)
}
