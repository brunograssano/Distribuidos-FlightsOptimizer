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

func (q *ProducerQueueProtocolHandler) DoCheckpoint(errors chan error) {
	log.Debugf("ProducerQueueProtocolHandler | Performing checkpoint")
	fileWriter, err := filemanager.NewFileWriter(fmt.Sprintf(tmpFile))
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

func (q *ProducerQueueProtocolHandler) Commit() {
	err := filemanager.DeleteFile(oldFile)
	if err != nil {
		log.Errorf("ProducerQueueProtocolHandler | Error deleting old file | %v", err)
	}
	err = filemanager.RenameFile(currFile, oldFile)
	if err != nil {
		log.Errorf("ProducerQueueProtocolHandler | Error renaming current checkpoint file | %v", err)
	}
	err = filemanager.RenameFile(tmpFile, currFile)
	if err != nil {
		log.Errorf("ProducerQueueProtocolHandler | Error renaming tmp file to current | %v", err)
	}
}

func (q *ProducerQueueProtocolHandler) Abort() {
	err := filemanager.DeleteFile(tmpFile)
	if err != nil {
		log.Errorf("ProducerQueueProtocolHandler | Error deleting tmp file | %v", err)
	}
}

func (q *ProducerQueueProtocolHandler) RestoreCheckpoint(typeOfRecovery checkpointer.CheckpointType) {
	if typeOfRecovery == checkpointer.Old {
		if q.HasPendingCheckpoints() {
			checkpointer.CopyOldIntoCurrent(oldFile, currFile)
		}
		q.readCheckpointAsState(oldFile)
	} else if typeOfRecovery == checkpointer.Curr {
		q.readCheckpointAsState(currFile)
	}
}

func (q *ProducerQueueProtocolHandler) HasPendingCheckpoints() bool {
	return filemanager.DirectoryExists(tmpFile)
}

func (q *ProducerQueueProtocolHandler) readCheckpointAsState(fileToRestore string) {
	log.Infof("ProducerQueueProtocolHandler | Restoring checkpoint: %v", fileToRestore)
	fileReader, err := filemanager.NewFileReader(fileToRestore)
	if err != nil {
		log.Fatalf("ProducerQueueProtocolHandler | Error trying to read checkpoint file | %v", err)
	}
	defer utils.CloseFileAndNotifyError(fileReader)
	for fileReader.CanRead() {
		line := fileReader.ReadLine()
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
	log.Infof("ProducerQueueProtocolHandler | Restored checkpoint successfully")
}
