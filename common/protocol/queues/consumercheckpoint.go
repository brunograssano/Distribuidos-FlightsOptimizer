package queues

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
	log "github.com/sirupsen/logrus"
)

const oldFileCons = "consumer_chk_old.csv"
const currFileCons = "consumer_chk_curr.csv"
const tmpFileCons = "consumer_chk_tmp.csv"

func (q *ConsumerQueueProtocolHandler) DoCheckpoint(errors chan error, id int, chkId int) {
	duplicatesResponse := make(chan error, 1)
	q.duplicatesHandler.DoCheckpoint(duplicatesResponse, id, chkId)
	<-duplicatesResponse
	checkpointer.DoCheckpointWithParser(errors, id, NewQueueProtocolCheckpointWriter(q.consumedByClients), q.consumer.GetName(), tmpFileCons, chkId)
}

func (q *ConsumerQueueProtocolHandler) Commit(id int, response chan error) {
	duplicatesResponse := make(chan error, 1)
	q.duplicatesHandler.Commit(id, duplicatesResponse)
	queueName := q.consumer.GetName()
	log.Debugf("ConsumerQueueProtocolHandler | Commiting checkpoint for id: %v_%v", id, q.consumer.GetName())
	checkpointer.HandleOldFile(id, queueName, oldFileCons)
	checkpointer.HandleCurrFile(id, queueName, currFileCons, oldFileCons)
	checkpointer.HandleTmpFile(id, queueName, tmpFileCons, currFileCons)
	<-duplicatesResponse
	response <- nil
}

func (q *ConsumerQueueProtocolHandler) Abort(id int, response chan error) {
	checkpointer.DeleteTmpFile(id, q.consumer.GetName(), tmpFileCons)
	duplicatesResponse := make(chan error, 1)
	q.duplicatesHandler.Abort(id, duplicatesResponse)
	<-duplicatesResponse
	response <- nil
}

func (q *ConsumerQueueProtocolHandler) RestoreCheckpoint(checkpointToRestore int, id int, result chan error) {
	duplicatesResult := make(chan error, 1)
	q.duplicatesHandler.RestoreCheckpoint(checkpointToRestore, id, duplicatesResult)
	<-duplicatesResult
	checkpointIds := checkpointer.GetCurrentValidCheckpoints(id, q.consumer.GetName(), currFileCons, oldFileCons)
	filesArray := []string{
		fmt.Sprintf("%v_%v_%v", id, q.consumer.GetName(), oldFileCons),
		fmt.Sprintf("%v_%v_%v", id, q.consumer.GetName(), currFileCons),
	}
	for idx, chkId := range checkpointIds {
		if chkId == checkpointToRestore {
			readCheckpointAsState(filesArray[idx], q.consumedByClients)
			break
		}
	}
	result <- nil
}

func (q *ConsumerQueueProtocolHandler) GetCheckpointVersions(id int) [2]int {
	return checkpointer.GetCurrentValidCheckpoints(id, q.consumer.GetName(), currFileCons, oldFileCons)
}
