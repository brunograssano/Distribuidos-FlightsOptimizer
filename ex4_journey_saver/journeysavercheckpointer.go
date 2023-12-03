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

const oldFileJs = "journey_saver_chk_old.csv"
const currFileJs = "journey_saver_chk_curr.csv"
const tmpFileJs = "journey_saver_chk_tmp.csv"

func (j *JourneySaver) DoCheckpoint(response chan error, id int, chkId int) {
	checkpointer.DoCheckpointWithParser(response, id, j, "", tmpFileJs, chkId)
}

func (j *JourneySaver) RestoreCheckpoint(checkpointToRestore int, id int, responses chan error) {
	checkpointIds := checkpointer.GetCurrentValidCheckpoints(id, "", currFileJs, oldFileJs)
	filesArray := []string{
		fmt.Sprintf("%v_%v_%v", id, "", oldFileJs),
		fmt.Sprintf("%v_%v_%v", id, "", currFileJs),
	}
	for idx, chkId := range checkpointIds {
		if chkId == checkpointToRestore {
			j.readCheckpointAsState(filesArray[idx])
			break
		}
	}
	responses <- nil
}

func (j *JourneySaver) GetCheckpointVersions(id int) [2]int {
	return checkpointer.GetCurrentValidCheckpoints(id, "", currFileJs, oldFileJs)
}

func (j *JourneySaver) Commit(id int, responses chan error) {
	log.Debugf("JourneySaver %v | Commiting checkpoint for id: %v", j.id, id)
	checkpointer.HandleOldFile(id, "", oldFileJs)
	checkpointer.HandleCurrFile(id, "", currFileJs, oldFileJs)
	checkpointer.HandleTmpFile(id, "", tmpFileJs, currFileJs)
	responses <- nil
}

func (j *JourneySaver) Abort(id int, responses chan error) {
	checkpointer.DeleteTmpFile(id, "", tmpFileJs)
	responses <- nil
}

func (j *JourneySaver) GetCheckpointString() string {
	linesToWrite := strings.Builder{}
	for client, partialResult := range j.partialResultsByClient {
		linesToWrite.WriteString(fmt.Sprintf("%v@%v@\n", client, partialResult.Serialize()))
	}
	for client, processed := range j.processedClients {
		linesToWrite.WriteString(fmt.Sprintf("%v@@%v\n", client, processed))
	}
	return linesToWrite.String()
}

func (j *JourneySaver) readCheckpointAsState(fileToRestore string) {
	if !filemanager.DirectoryExists(fileToRestore) {
		log.Infof("JourneySaver %v | Does not have a checkpoint: %v", j.id, fileToRestore)
		return
	}
	log.Infof("JourneySaver %v | Restoring checkpoint: %v", j.id, fileToRestore)
	fileReader, err := filemanager.NewFileReader(fileToRestore)
	if err != nil {
		log.Fatalf("JourneySaver %v | Error trying to read checkpoint file: %v | %v", j.id, fileToRestore, err)
	}
	defer utils.CloseFileAndNotifyError(fileReader)

	filemanager.SkipHeader(fileReader)
	for fileReader.CanRead() {
		line := fileReader.ReadLine()
		clientIdPRAndProcessed := strings.Split(line, utils.AtSeparator)
		if len(clientIdPRAndProcessed) != 3 {
			log.Errorf("JourneySaver %v | Error deserializing checkpoint | %v", j.id, clientIdPRAndProcessed)
			continue
		}
		clientId := clientIdPRAndProcessed[0]
		if clientIdPRAndProcessed[1] != "" {
			partialResult, err := DeserializePartialResultFromStr(clientIdPRAndProcessed[1])
			if err != nil {
				log.Errorf("JourneySaver %v | Error deserializing partial result, skippping line | %v", j.id, err)
				continue
			}
			j.partialResultsByClient[clientId] = partialResult
		}
		if clientIdPRAndProcessed[2] != "" {
			processed, err := strconv.ParseBool(clientIdPRAndProcessed[2])
			if err != nil {
				log.Errorf("JourneySaver %v | Error converting processed into bool, skipping line | %v", j.id, err)
				continue
			}
			j.processedClients[clientId] = processed
		}
	}
	err = fileReader.Err()
	if err != nil {
		log.Errorf("JourneySaver %v | Error reading from checkpoint: %v | %v", j.id, fileToRestore, err)
	}
	log.Infof("JourneySaver %v | Restored checkpoint successfully: %v | State recovered: %v - %v", j.id, fileToRestore, j.partialResultsByClient, j.processedClients)
}
