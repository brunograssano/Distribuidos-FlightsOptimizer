package queues

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	log "github.com/sirupsen/logrus"
)

type ConsumerQueueProtocolHandler struct {
	consumer  middleware.ConsumerInterface
	recvCount int
	status    bool
	lastMsg   *dataStructures.Message
}

func NewConsumerQueueProtocolHandler(consumer middleware.ConsumerInterface) *ConsumerQueueProtocolHandler {
	return &ConsumerQueueProtocolHandler{
		consumer:  consumer,
		recvCount: 0,
		lastMsg:   nil,
	}
}

func (q *ConsumerQueueProtocolHandler) Pop() (*dataStructures.Message, bool) {
	err := q.notifyStatusOfLastMessage()
	if err != nil {
		log.Errorf("ConsumerQueueProtocolHandler | Error notifying status of last message | %v", err)
	}

	bytes, ok := q.consumer.Pop()
	if !ok {
		return nil, ok
	}
	msg := serializer.DeserializeMsg(bytes)
	q.lastMsg = msg
	return msg, ok
}

func (q *ConsumerQueueProtocolHandler) GetReceivedMessages() int {
	return q.recvCount
}

func (q *ConsumerQueueProtocolHandler) ClearData() {
	q.recvCount = 0
}

func (q *ConsumerQueueProtocolHandler) SetStatusOfLastMessage(status bool) {
	q.status = status
}

func (q *ConsumerQueueProtocolHandler) notifyStatusOfLastMessage() error {
	err := q.consumer.SignalFinishedMessage(q.status)
	if err != nil {
		log.Errorf("ConsumerProtocolHandler | Error trying to notify status of last message | %v", err)
		return err
	}
	if q.lastMsg != nil && q.lastMsg.TypeMessage == dataStructures.FlightRows && q.status {
		q.recvCount += len(q.lastMsg.DynMaps)
	}
	q.status = true
	return nil
}
