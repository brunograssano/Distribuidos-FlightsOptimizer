package protocol

import (
	"github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
)

type ConsumerProtocolInterface interface {
	DataCleaner
	Pop() (*data_structures.Message, bool)
	GetReceivedMessages() int
}

type ConsumerQueueProtocolHandler struct {
	consumer  middleware.ConsumerInterface
	recvCount int
}

func NewConsumerQueueProtocolHandler(consumer middleware.ConsumerInterface) *ConsumerQueueProtocolHandler {
	return &ConsumerQueueProtocolHandler{
		consumer:  consumer,
		recvCount: 0,
	}
}

func (q *ConsumerQueueProtocolHandler) Pop() (*data_structures.Message, bool) {
	bytes, ok := q.consumer.Pop()
	if !ok {
		return nil, ok
	}
	msg := serializer.DeserializeMsg(bytes)
	if msg.TypeMessage == data_structures.FlightRows {
		q.recvCount += len(msg.DynMaps)
	}
	return msg, ok
}

func (q *ConsumerQueueProtocolHandler) GetReceivedMessages() int {
	return q.recvCount
}

func (q *ConsumerQueueProtocolHandler) ClearData() {
	q.recvCount = 0
}
