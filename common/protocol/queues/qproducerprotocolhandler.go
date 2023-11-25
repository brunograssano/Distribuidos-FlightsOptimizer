package queues

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	log "github.com/sirupsen/logrus"
)

type ProducerQueueProtocolHandler struct {
	producer          middleware.ProducerInterface
	totalSentByClient map[string]int
}

const oldFile = "producer_chk_old.csv"
const currFile = "producer_chk.csv"
const tmpFile = "producer_chk_tmp.csv"

func NewProducerQueueProtocolHandler(producer middleware.ProducerInterface) *ProducerQueueProtocolHandler {
	return &ProducerQueueProtocolHandler{
		producer:          producer,
		totalSentByClient: make(map[string]int),
	}
}

func (q *ProducerQueueProtocolHandler) Send(msg *dataStructures.Message) error {
	bytes := serializer.SerializeMsg(msg)
	if msg.TypeMessage == dataStructures.FlightRows {
		_, exists := q.totalSentByClient[msg.ClientId]
		if !exists {
			q.totalSentByClient[msg.ClientId] = 0
		}
		q.totalSentByClient[msg.ClientId] += len(msg.DynMaps)
	}
	return q.producer.Send(bytes)
}

func (q *ProducerQueueProtocolHandler) GetSentMessages(clientId string) int {
	count, exists := q.totalSentByClient[clientId]
	if !exists {
		log.Warnf("ProducerQueueProtocolHandler | Warning Message | Client Id %v not found. Returnin 0 for sent messages.", clientId)
		return 0
	}
	return count
}

func (q *ProducerQueueProtocolHandler) ClearData(clientId string) {
	delete(q.totalSentByClient, clientId)
}
