package main

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

const oldFileSink = "sink_chk_old.csv"
const currFileSink = "sink_chk_curr.csv"
const tmpFileSink = "sink_chk_tmp.csv"

func (j *JourneySink) DoCheckpoint(response chan error, id int, chkId int) {
	checkpointer.DoCheckpointWithParser(response, id, j, "", tmpFileSink, chkId)
}

func (j *JourneySink) RestoreCheckpoint(checkpointToRestore int, id int, responses chan error) {
	checkpointIds := checkpointer.GetCurrentValidCheckpoints(id, "", currFileSink, oldFileSink)
	filesArray := []string{
		fmt.Sprintf("%v_%v_%v", id, "", oldFileSink),
		fmt.Sprintf("%v_%v_%v", id, "", currFileSink),
	}
	for idx, chkId := range checkpointIds {
		if chkId == checkpointToRestore {
			j.readCheckpointAsState(filesArray[idx])
			break
		}
	}
	responses <- nil
}

func (j *JourneySink) GetCheckpointVersions(id int) [2]int {
	return checkpointer.GetCurrentValidCheckpoints(id, "", currFileSink, oldFileSink)
}

func (j *JourneySink) Commit(id int, responses chan error) {
	log.Debugf("JourneySink | Commiting checkpoint for id: %v", id)
	checkpointer.HandleOldFile(id, "", oldFileSink)
	checkpointer.HandleCurrFile(id, "", currFileSink, oldFileSink)
	checkpointer.HandleTmpFile(id, "", tmpFileSink, currFileSink)
	responses <- nil
}

func (j *JourneySink) Abort(id int, responses chan error) {
	checkpointer.DeleteTmpFile(id, "", tmpFileSink)
	responses <- nil
}

func (j *JourneySink) GetCheckpointString() string {
	linesToWrite := strings.Builder{}
	for client, received := range j.journeySaversReceivedByClient {
		linesToWrite.WriteString(fmt.Sprintf("%v=%v\n", client, received))
	}
	return linesToWrite.String()
}

func (j *JourneySink) readCheckpointAsState(fileToRestore string) {
	if !filemanager.DirectoryExists(fileToRestore) {
		log.Infof("JourneySink | Does not have a checkpoint: %v", fileToRestore)
		return
	}
	log.Infof("JourneySink | Restoring checkpoint: %v", fileToRestore)
	fileReader, err := filemanager.NewFileReader(fileToRestore)
	if err != nil {
		log.Fatalf("JourneySink | Error trying to read checkpoint file: %v | %v", fileToRestore, err)
	}
	defer utils.CloseFileAndNotifyError(fileReader)

	filemanager.SkipHeader(fileReader)
	for fileReader.CanRead() {
		line := fileReader.ReadLine()
		clientIdAndValue := strings.Split(line, "=")
		if len(clientIdAndValue) != 2 {
			log.Errorf("JourneySink | Error deserializing checkpoint | %v", clientIdAndValue)
			continue
		}
		clientId := clientIdAndValue[0]
		jsReceivedByClient, err := strconv.Atoi(clientIdAndValue[1])
		if err != nil {
			log.Errorf("JourneySink | Error converting savers received by client into int when checkpointing | %v", err)
			continue
		}
		j.journeySaversReceivedByClient[clientId] = uint(jsReceivedByClient)
	}
	err = fileReader.Err()
	if err != nil {
		log.Errorf("JourneySink | Error reading from checkpoint: %v | %v", fileToRestore, err)
	}
	log.Infof("JourneySink | Restored checkpoint successfully: %v | State recovered: %v", fileToRestore, j.journeySaversReceivedByClient)
}
