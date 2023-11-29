package queues

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

const oldFile = "producer_chk_old.csv"
const currFile = "producer_chk_curr.csv"
const tmpFile = "producer_chk_tmp.csv"

func (q *ProducerQueueProtocolHandler) DoCheckpoint(errors chan error, id int) {
	log.Debugf("ProducerQueueProtocolHandler | Performing checkpoint")
	fileWriter, err := filemanager.NewFileWriter(fmt.Sprintf("%v_%v_%v", id, q.producer.GetName(), tmpFile))
	if err != nil {
		log.Errorf("ProducerQueueProtocolHandler | Error trying to create tmp checkpoint file | %v", err)
		errors <- err
		return
	}
	defer utils.CloseFileAndNotifyError(fileWriter)
	linesToWrite := strings.Builder{}
	for clientId, totalSent := range q.totalSentByClient {
		linesToWrite.WriteString(fmt.Sprintf("%v=%v\n", clientId, totalSent))
	}
	err = fileWriter.WriteLine(linesToWrite.String())
	if err != nil {
		log.Errorf("ProducerQueueProtocolHandler | Error trying to write the checkpoint | %v", err)
		errors <- err
		return
	}
	errors <- nil
}

func (q *ProducerQueueProtocolHandler) Commit(id int, response chan error) {
	log.Debugf("ProducerQueueProtocolHandler | Commiting checkpoint for id: %v_%v", id, q.producer.GetName())
	q.handleOldFile(id)
	q.handleCurrFile(id)
	q.handleTmpFile(id)
	response <- nil
}

func (q *ProducerQueueProtocolHandler) handleTmpFile(id int) {
	tmpFileName := fmt.Sprintf("%v_%v_%v", id, q.producer.GetName(), tmpFile)
	currFileName := fmt.Sprintf("%v_%v_%v", id, q.producer.GetName(), currFile)
	if !filemanager.DirectoryExists(tmpFileName) {
		log.Errorf("ProducerQueueProtocolHandler | Tmp file %v does not exist", tmpFileName)
		return
	}
	log.Debugf("ProducerQueueProtocolHandler | Renaming TMP Checkpoint into Current")
	err := filemanager.RenameFile(tmpFileName, currFileName)
	if err != nil {
		log.Errorf("ProducerQueueProtocolHandler | Error renaming tmp file to current | %v", err)
	}
}

func (q *ProducerQueueProtocolHandler) handleCurrFile(id int) {
	currFileName := fmt.Sprintf("%v_%v_%v", id, q.producer.GetName(), currFile)
	oldFileName := fmt.Sprintf("%v_%v_%v", id, q.producer.GetName(), oldFile)
	if !filemanager.DirectoryExists(currFileName) {
		log.Debugf("ProducerQueueProtocolHandler | CurrFile %v does not exist", currFileName)
		return
	}
	log.Debugf("ProducerQueueProtocolHandler | Renaming Current into Old Checkpoint")
	err := filemanager.RenameFile(currFileName, oldFileName)
	if err != nil {
		log.Errorf("ProducerQueueProtocolHandler | Error renaming current checkpoint file | %v", err)
	}
}

func (q *ProducerQueueProtocolHandler) handleOldFile(id int) {
	oldFileName := fmt.Sprintf("%v_%v_%v", id, q.producer.GetName(), oldFile)
	if !filemanager.DirectoryExists(oldFileName) {
		log.Debugf("ProducerQueueProtocolHandler | OldFile %v does not exist", oldFileName)
		return
	}
	log.Debugf("ProducerQueueProtocolHandler | Deleting Old Checkpoint")
	err := filemanager.DeleteFile(oldFileName)
	if err != nil {
		log.Errorf("ProducerQueueProtocolHandler | Error deleting old file | %v", err)
	}
}

func (q *ProducerQueueProtocolHandler) Abort(id int, response chan error) {
	tmpFileName := fmt.Sprintf("%v_%v_%v", id, q.producer.GetName(), tmpFile)
	log.Debugf("ProducerQueueProtocolHandler | Aborting Checkpoint | Deleting TMP Checkpoint File")
	err := filemanager.DeleteFile(tmpFileName)
	if err != nil {
		log.Errorf("ProducerQueueProtocolHandler | Error deleting tmp file | %v", err)
	}
	response <- nil
}

func (q *ProducerQueueProtocolHandler) RestoreCheckpoint(typeOfRecovery checkpointer.CheckpointType, id int, result chan error) {
	oldFileName := fmt.Sprintf("%v_%v_%v", id, q.producer.GetName(), oldFile)
	currFileName := fmt.Sprintf("%v_%v_%v", id, q.producer.GetName(), currFile)
	if typeOfRecovery == checkpointer.Old {
		if q.HasPendingCheckpoints(id) {
			checkpointer.CopyOldIntoCurrent(oldFileName, currFileName)
			tmpFile := fmt.Sprintf("%v_%v_%v", id, q.producer.GetName(), tmpFile)
			err := filemanager.DeleteFile(tmpFile)
			if err != nil {
				log.Errorf("ProducerQueueProtocolHandler %v | Error deleting TMP file when restoring checkpoint: %v", id, tmpFile)
			}
		}
		q.readCheckpointAsState(oldFileName)
	} else if typeOfRecovery == checkpointer.Curr {
		q.readCheckpointAsState(currFileName)
	}
	result <- nil
}

func (q *ProducerQueueProtocolHandler) HasPendingCheckpoints(id int) bool {
	return filemanager.DirectoryExists(fmt.Sprintf("%v_%v_%v", id, q.producer.GetName(), tmpFile))
}

func (q *ProducerQueueProtocolHandler) readCheckpointAsState(fileToRestore string) {
	if !filemanager.DirectoryExists(fileToRestore) {
		log.Infof("ProducerQueueProtocolHandler | Does not have a checkpoint")
		return
	}
	log.Infof("ProducerQueueProtocolHandler | Restoring checkpoint: %v", fileToRestore)
	fileReader, err := filemanager.NewFileReader(fileToRestore)
	if err != nil {
		log.Fatalf("ProducerQueueProtocolHandler | Error trying to read checkpoint file | %v", err)
	}
	defer utils.CloseFileAndNotifyError(fileReader)
	for fileReader.CanRead() {
		line := fileReader.ReadLine()
		log.Debugf("Linea leida del checkpoint: %v", line)
		clientIdAndSent := strings.Split(line, "=")
		clientId := clientIdAndSent[0]
		sent, err := strconv.Atoi(clientIdAndSent[1])
		if err != nil {
			log.Errorf("ProducerQueueProtocolHandler | On Checkpointing | Error trying to convert sent to int: %v | %v", sent, err)
		}
		q.totalSentByClient[clientId] = sent
	}
	err = fileReader.Err()
	if err != nil {
		log.Errorf("ProducerQueueProtocolHandler | Error reading from checkpoint | %v", err)
	}
	log.Infof("ProducerQueueProtocolHandler | Restored checkpoint successfully | State recovered: %v", q.totalSentByClient)
}
