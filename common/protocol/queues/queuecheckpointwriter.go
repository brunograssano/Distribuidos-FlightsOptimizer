package queues

import (
	"fmt"
	"strings"
)

type QueueProtocolCheckpointWriter struct {
	dataToWrite map[string]int
}

func NewQueueProtocolCheckpointWriter(dataToWrite map[string]int) *QueueProtocolCheckpointWriter {
	return &QueueProtocolCheckpointWriter{
		dataToWrite,
	}
}

func (q QueueProtocolCheckpointWriter) GetCheckpointString() string {
	linesToWrite := strings.Builder{}
	for clientId, totalSent := range q.dataToWrite {
		linesToWrite.WriteString(fmt.Sprintf("%v=%v\n", clientId, totalSent))
	}
	return linesToWrite.String()
}
