package protocol

import (
	"github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
)

type ConsumerProtocolInterface interface {
	Pop() (*data_structures.Message, bool)
}

type ConsumerQueueProtocolHandler struct {
	consumer   middleware.ConsumerInterface
	serializer *data_structures.Serializer
}

func NewConsumerQueueProtocolHandler(consumer middleware.ConsumerInterface) *ConsumerQueueProtocolHandler {
	return &ConsumerQueueProtocolHandler{
		consumer:   consumer,
		serializer: data_structures.NewSerializer(),
	}
}

func (q *ConsumerQueueProtocolHandler) Pop() (*data_structures.Message, bool) {
	bytes, ok := q.consumer.Pop()
	msg := q.serializer.DeserializeMsg(bytes)
	return msg, ok
}
