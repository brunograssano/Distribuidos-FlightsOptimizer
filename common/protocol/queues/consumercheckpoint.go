package queues

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	log "github.com/sirupsen/logrus"
)

const oldFileCons = "consumer_chk_old.csv"
const currFileCons = "consumer_chk_curr.csv"
const tmpFileCons = "consumer_chk_tmp.csv"

func (q *ConsumerQueueProtocolHandler) DoCheckpoint(errors chan error, id int) {
	duplicatesResponse := make(chan error, 1)
	q.duplicatesHandler.DoCheckpoint(duplicatesResponse, id)
	<-duplicatesResponse
	checkpointer.DoCheckpointWithParser(errors, id, NewQueueProtocolCheckpointWriter(q.consumedByClients), q.consumer.GetName(), tmpFileCons)
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
	checkpointer.DeleteTmpFile(id, q.consumer.GetName(), tmpFileProd)
	duplicatesResponse := make(chan error, 1)
	q.duplicatesHandler.Abort(id, duplicatesResponse)
	<-duplicatesResponse
	response <- nil
}

func (q *ConsumerQueueProtocolHandler) RestoreCheckpoint(typeOfRecovery checkpointer.CheckpointType, id int, result chan error) {
	oldFileName := fmt.Sprintf("%v_%v_%v", id, q.consumer.GetName(), oldFileCons)
	currFileName := fmt.Sprintf("%v_%v_%v", id, q.consumer.GetName(), currFileCons)
	if !filemanager.DirectoryExists(currFileName) && !filemanager.DirectoryExists(oldFileName) {
		result <- nil
		return
	}
	duplicatesResult := make(chan error, 1)
	q.duplicatesHandler.RestoreCheckpoint(typeOfRecovery, id, duplicatesResult)
	fileToRead := checkpointer.GetFileToRead(typeOfRecovery, id, q.consumer.GetName(), oldFileCons, currFileCons, tmpFileCons)
	readCheckpointAsState(fileToRead, q.consumedByClients)
	<-duplicatesResult
	result <- nil
}

func (q *ConsumerQueueProtocolHandler) HasPendingCheckpoints(id int) bool {
	return checkpointer.PendingCheckpointExists(id, q.consumer.GetName(), tmpFileProd)
}
