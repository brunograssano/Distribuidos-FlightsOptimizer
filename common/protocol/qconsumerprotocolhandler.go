package protocol

import (
	"github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
)

type ConsumerProtocolInterface interface {
	Pop() (*data_structures.Message, bool)
	GetReceivedMessages() int
}

type ConsumerQueueProtocolHandler struct {
	consumer   middleware.ConsumerInterface
	serializer *data_structures.Serializer
	recvCount  int
}

func NewConsumerQueueProtocolHandler(consumer middleware.ConsumerInterface) *ConsumerQueueProtocolHandler {
	return &ConsumerQueueProtocolHandler{
		consumer:   consumer,
		serializer: data_structures.NewSerializer(),
		recvCount:  0,
	}
}

func (q *ConsumerQueueProtocolHandler) Pop() (*data_structures.Message, bool) {
	bytes, ok := q.consumer.Pop()
	msg := q.serializer.DeserializeMsg(bytes)
	if msg.TypeMessage == data_structures.FlightRows {
		q.recvCount += len(msg.DynMaps)
	}
	return msg, ok
}

func (q *ConsumerQueueProtocolHandler) GetReceivedMessages() int {
	return q.recvCount
}
