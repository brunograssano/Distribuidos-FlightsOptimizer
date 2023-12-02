package main

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"strings"
)

const oldFileAccum = "accum_chk_old.csv"
const currFileAccum = "accum_chk_curr.csv"
const tmpFileAccum = "accum_chk_tmp.csv"

func (a *AvgCalculator) DoCheckpoint(response chan error, id int, chkId int) {
	checkpointer.DoCheckpointWithParser(response, id, a, "", tmpFileAccum, chkId)
}

func (a *AvgCalculator) RestoreCheckpoint(checkpointToRestore int, id int, response chan error) {
	checkpointIds := checkpointer.GetCurrentValidCheckpoints(id, "", currFileAccum, oldFileAccum)
	filesArray := []string{
		fmt.Sprintf("%v_%v_%v", id, "", oldFileAccum),
		fmt.Sprintf("%v_%v_%v", id, "", currFileAccum),
	}
	for idx, chkId := range checkpointIds {
		if chkId == checkpointToRestore {
			a.readCheckpointAsState(filesArray[idx])
			break
		}
	}
	response <- nil
}

func (a *AvgCalculator) GetCheckpointVersions(id int) [2]int {
	return checkpointer.GetCurrentValidCheckpoints(id, "", currFileAccum, oldFileAccum)
}

func (a *AvgCalculator) Commit(id int, response chan error) {
	log.Debugf("AvgCalculator | Commiting checkpoint for id: %v", id)
	checkpointer.HandleOldFile(id, "", oldFileAccum)
	checkpointer.HandleCurrFile(id, "", currFileAccum, oldFileAccum)
	checkpointer.HandleTmpFile(id, "", tmpFileAccum, currFileAccum)
	response <- nil
}

func (a *AvgCalculator) Abort(id int, response chan error) {
	checkpointer.DeleteTmpFile(id, "", tmpFileAccum)
	response <- nil
}

func (a *AvgCalculator) GetCheckpointString() string {
	linesToWrite := strings.Builder{}
	for client, partialSum := range a.valuesReceivedByClient {
		linesToWrite.WriteString(fmt.Sprintf("%v=%v\n", client, partialSum.Serialize()))
	}
	return linesToWrite.String()
}

func (a *AvgCalculator) readCheckpointAsState(fileToRestore string) {
	if !filemanager.DirectoryExists(fileToRestore) {
		log.Infof("AvgCalculator | Does not have a checkpoint: %v", fileToRestore)
		return
	}
	log.Infof("AvgCalculator | Restoring checkpoint: %v", fileToRestore)
	fileReader, err := filemanager.NewFileReader(fileToRestore)
	if err != nil {
		log.Fatalf("AvgCalculator | Error trying to read checkpoint file: %v | %v", fileToRestore, err)
	}
	defer utils.CloseFileAndNotifyError(fileReader)

	filemanager.SkipHeader(fileReader)
	for fileReader.CanRead() {
		line := fileReader.ReadLine()
		clientIdAndPartialSum := strings.Split(line, "=")
		clientId := clientIdAndPartialSum[0]
		partialSumString := clientIdAndPartialSum[1]
		partialSum, err := DeserializePartialSum(partialSumString)
		if err != nil {
			log.Errorf("AvgCalculator | Error deserializing partial sum for client id %v | %v", clientId, err)
			continue
		}
		a.valuesReceivedByClient[clientId] = *partialSum
	}
	err = fileReader.Err()
	if err != nil {
		log.Errorf("AvgCalculator | Error reading from checkpoint: %v | %v", fileToRestore, err)
	}
	log.Infof("AvgCalculator | Restored checkpoint successfully: %v | State recovered: %v", fileToRestore, a.valuesReceivedByClient)
}
