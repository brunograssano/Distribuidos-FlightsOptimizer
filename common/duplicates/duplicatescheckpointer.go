package duplicates

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

const oldFileDup = "duplicates_chk_old.csv"
const currFileDup = "duplicates_chk_curr.csv"
const tmpFileDup = "duplicates_chk_tmp.csv"

func (dh *DuplicatesHandler) DoCheckpoint(errors chan error, id int, chkId int) {
	checkpointer.DoCheckpointWithParser(errors, id, dh, dh.queueName, tmpFileDup, chkId)
}

func (dh *DuplicatesHandler) Commit(id int, response chan error) {
	log.Debugf("DuplicatesHandler | Commiting checkpoint for id: %v_%v", id, dh.queueName)
	checkpointer.HandleOldFile(id, dh.queueName, oldFileDup)
	checkpointer.HandleCurrFile(id, dh.queueName, currFileDup, oldFileDup)
	checkpointer.HandleTmpFile(id, dh.queueName, tmpFileDup, currFileDup)
	response <- nil
}

func (dh *DuplicatesHandler) Abort(id int, response chan error) {
	checkpointer.DeleteTmpFile(id, dh.queueName, tmpFileDup)
	response <- nil
}

func (dh *DuplicatesHandler) RestoreCheckpoint(checkpointToRestore int, id int, result chan error) {
	checkpointIds := checkpointer.GetCurrentValidCheckpoints(id, dh.queueName, currFileDup, oldFileDup)
	filesArray := []string{
		fmt.Sprintf("%v_%v_%v", id, dh.queueName, oldFileDup),
		fmt.Sprintf("%v_%v_%v", id, dh.queueName, currFileDup),
	}
	for idx, chkId := range checkpointIds {
		if chkId == checkpointToRestore {
			dh.readCheckpointAsState(filesArray[idx])
			break
		}
	}
	result <- nil
}

func (dh *DuplicatesHandler) readCheckpointAsState(fileToRestore string) {
	if !filemanager.DirectoryExists(fileToRestore) {
		log.Infof("DuplicatesHandler | Does not have a checkpoint: %v", fileToRestore)
		return
	}
	log.Infof("DuplicatesHandler | Restoring checkpoint: %v", fileToRestore)
	fileReader, err := filemanager.NewFileReader(fileToRestore)
	if err != nil {
		log.Fatalf("DuplicatesHandler | Error trying to read checkpoint file: %v | %v", fileToRestore, err)
	}
	defer utils.CloseFileAndNotifyError(fileReader)

	filemanager.SkipHeader(fileReader)
	for fileReader.CanRead() {
		line := fileReader.ReadLine()
		clientIdAndMessages := strings.Split(line, ",")
		clientId := clientIdAndMessages[0]
		dh.lastMessagesSeen[clientId] = make(map[uint]uint16)
		//Avoid reading the first registry that is client id
		for i := 1; i < len(clientIdAndMessages); i++ {
			messageIdAndRowId := strings.Split(clientIdAndMessages[i], "=")
			msgId, err := strconv.Atoi(messageIdAndRowId[0])
			if err != nil {
				log.Errorf("DuplicatesHandler | On Checkpointing %v | Error trying to convert msgId to int: %v | %v", fileToRestore, msgId, err)
			}
			rowId, err := strconv.Atoi(messageIdAndRowId[1])
			if err != nil {
				log.Errorf("DuplicatesHandler | On Checkpointing %v | Error trying to convert rowId to int: %v | %v", fileToRestore, rowId, err)
			}
			dh.lastMessagesSeen[clientId][uint(msgId)] = uint16(rowId)
		}
	}
	err = fileReader.Err()
	if err != nil {
		log.Errorf("DuplicatesHandler | Error reading from checkpoint: %v | %v", fileToRestore, err)
	}
	log.Infof("DuplicatesHandler | Restored checkpoint successfully: %v | State recovered: %v", fileToRestore, dh.lastMessagesSeen)
}

func (dh *DuplicatesHandler) GetCheckpointVersions(id int) [2]int {
	return checkpointer.GetCurrentValidCheckpoints(id, dh.queueName, currFileDup, oldFileDup)
}
