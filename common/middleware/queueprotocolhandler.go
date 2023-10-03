package middleware

import (
	"github.com/brunograssano/Distribuidos-TP1/common/data_structures"
)

type QueueProtocolHandler struct {
	consumer   ConsumerInterface
	producer   ProducerInterface
	serializer *data_structures.Serializer
}

func NewQueueProtocolHandler() *QueueProtocolHandler {
	return &QueueProtocolHandler{

		serializer: data_structures.NewSerializer(),
	}
}

func (q *QueueProtocolHandler) SendMsg(msg *data_structures.Message) error {
	// todo
	return nil
}

func (q *QueueProtocolHandler) ReadMsg() (*data_structures.Message, error) {
	// todo
	return nil, nil
}
