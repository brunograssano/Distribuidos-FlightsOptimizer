package queues

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/duplicates"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	log "github.com/sirupsen/logrus"
)

type ConsumerQueueProtocolHandler struct {
	consumer          middleware.ConsumerInterface
	status            bool
	lastMsg           *dataStructures.Message
	consumedByClients map[string]int
	duplicatesHandler duplicates.DuplicateDetector
}

func NewConsumerQueueProtocolHandler(consumer middleware.ConsumerInterface, duplicatesHandler duplicates.DuplicateDetector) *ConsumerQueueProtocolHandler {
	return &ConsumerQueueProtocolHandler{
		consumer:          consumer,
		lastMsg:           nil,
		consumedByClients: make(map[string]int),
		duplicatesHandler: duplicatesHandler,
	}
}

func (q *ConsumerQueueProtocolHandler) Pop() (*dataStructures.Message, bool) {
	var msg *dataStructures.Message
	for {
		err := q.notifyStatusOfLastMessage()
		if err != nil {
			log.Errorf("ConsumerQueueProtocolHandler | Error notifying status of last message | %v", err)
		}
		bytes, ok := q.consumer.Pop()
		if !ok {
			return nil, false
		}
		msg = serializer.DeserializeMsg(bytes)
		q.lastMsg = msg
		if !q.duplicatesHandler.IsDuplicate(msg) {
			break
		} else {
			log.Warnf("ConsumerQueueProtocolhandler | Got Duplicated Message: %v-%v-%v| Discarding it...", msg.ClientId, msg.MessageId, msg.RowId)
		}
	}
	return msg, true
}

func (q *ConsumerQueueProtocolHandler) GetReceivedMessages(clientId string) int {
	count, exists := q.consumedByClients[clientId]
	if !exists {
		log.Warnf("ConsumerQueueProtocolHandler | Warning Message | Client with id %v not found. Returning 0 for Recvd Messages.", clientId)
		return 0
	}
	return count
}

func (q *ConsumerQueueProtocolHandler) ClearData(clientId string) {
	delete(q.consumedByClients, clientId)
}

func (q *ConsumerQueueProtocolHandler) SetStatusOfLastMessage(status bool) {
	q.status = status
}

func (q *ConsumerQueueProtocolHandler) notifyStatusOfLastMessage() error {
	if q.status && q.lastMsg != nil {
		q.duplicatesHandler.SaveMessageSeen(q.lastMsg)
	}
	err := q.consumer.SignalFinishedMessage(q.status)
	if err != nil {
		log.Errorf("ConsumerProtocolHandler | Error trying to notify status of last message | %v", err)
		return err
	}
	if q.lastMsg != nil && q.lastMsg.TypeMessage == dataStructures.FlightRows && q.status {
		_, exists := q.consumedByClients[q.lastMsg.ClientId]
		if !exists {
			q.consumedByClients[q.lastMsg.ClientId] = 0
		}
		q.consumedByClients[q.lastMsg.ClientId] += len(q.lastMsg.DynMaps)
	}
	q.status = true
	return nil
}
