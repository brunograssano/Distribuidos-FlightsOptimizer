package queues

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
)

type ProducerProtocolInterface interface {
	protocol.DataCleaner
	Send(msg *dataStructures.Message) error
	GetSentMessages(string) int
}

type ConsumerProtocolInterface interface {
	protocol.DataCleaner
	Pop() (*dataStructures.Message, bool)
	GetReceivedMessages(string) int
	SetStatusOfLastMessage(bool)
}
