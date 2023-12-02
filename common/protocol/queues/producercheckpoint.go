package queues

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
	log "github.com/sirupsen/logrus"
)

const oldFileProd = "producer_chk_old.csv"
const currFileProd = "producer_chk_curr.csv"
const tmpFileProd = "producer_chk_tmp.csv"

func (q *ProducerQueueProtocolHandler) DoCheckpoint(errors chan error, id int, chkId int) {
	checkpointer.DoCheckpointWithParser(errors, id, NewQueueProtocolCheckpointWriter(q.totalSentByClient), q.producer.GetName(), tmpFileProd, chkId)
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

func (q *ProducerQueueProtocolHandler) RestoreCheckpoint(checkpointToRestore int, id int, result chan error) {
	checkpointIds := checkpointer.GetCurrentValidCheckpoints(id, q.producer.GetName(), currFileProd, oldFileProd)
	filesArray := []string{
		fmt.Sprintf("%v_%v_%v", id, q.producer.GetName(), oldFileProd),
		fmt.Sprintf("%v_%v_%v", id, q.producer.GetName(), currFileProd),
	}
	for idx, chkId := range checkpointIds {
		if chkId == checkpointToRestore {
			readCheckpointAsState(filesArray[idx], q.totalSentByClient)
			break
		}
	}
	result <- nil
}

func (q *ProducerQueueProtocolHandler) GetCheckpointVersions(id int) [2]int {
	return checkpointer.GetCurrentValidCheckpoints(id, q.producer.GetName(), currFileProd, oldFileProd)
}
