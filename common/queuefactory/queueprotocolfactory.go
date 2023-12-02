package queuefactory

import (
	"github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
)

type QueueProtocolFactory interface {
	CreateProducer(string) queues.ProducerProtocolInterface
	CreateConsumer(string, string) queues.ConsumerProtocolInterface
}
