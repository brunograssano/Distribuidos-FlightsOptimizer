package queues

import (
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
	log "github.com/sirupsen/logrus"
)

const oldFileProd = "producer_chk_old.csv"
const currFileProd = "producer_chk_curr.csv"
const tmpFileProd = "producer_chk_tmp.csv"

func (q *ProducerQueueProtocolHandler) DoCheckpoint(errors chan error, id int) {
	checkpointer.DoCheckpointWithParser(errors, id, NewQueueProtocolCheckpointWriter(q.totalSentByClient), q.producer.GetName(), tmpFileProd)
}

func (q *ProducerQueueProtocolHandler) Commit(id int, response chan error) {
	queueName := q.producer.GetName()
	log.Debugf("ProducerQueueProtocolHandler | Commiting checkpoint for id: %v_%v", id, q.producer.GetName())
	checkpointer.HandleOldFile(id, queueName, oldFileProd)
	checkpointer.HandleCurrFile(id, queueName, currFileProd, oldFileProd)
	checkpointer.HandleTmpFile(id, queueName, tmpFileProd, currFileProd)
	response <- nil
}

func (q *ProducerQueueProtocolHandler) Abort(id int, response chan error) {
	checkpointer.DeleteTmpFile(id, q.producer.GetName(), tmpFileProd)
	response <- nil
}

func (q *ProducerQueueProtocolHandler) RestoreCheckpoint(typeOfRecovery checkpointer.CheckpointType, id int, result chan error) {
	fileToRead := checkpointer.GetFileToRead(typeOfRecovery, id, q.producer.GetName(), oldFileProd, currFileProd, tmpFileProd)
	readCheckpointAsState(fileToRead, q.totalSentByClient)
	result <- nil
}

func (q *ProducerQueueProtocolHandler) HasPendingCheckpoints(id int) bool {
	return checkpointer.PendingCheckpointExists(id, q.producer.GetName(), tmpFileProd)
}
