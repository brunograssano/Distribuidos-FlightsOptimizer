package queues

import (
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
)

type ProducerProtocolInterface interface {
	checkpointer.Checkpointable
	protocol.DataCleaner
	Send(msg *dataStructures.Message) error
	GetSentMessages(string) int
}

type ConsumerProtocolInterface interface {
	checkpointer.Checkpointable
	protocol.DataCleaner
	Pop() (*dataStructures.Message, bool)
	GetReceivedMessages(string) int
	SetStatusOfLastMessage(bool)
}
