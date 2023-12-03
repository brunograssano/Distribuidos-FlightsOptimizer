package ex3

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

const oldFileSaver = "saver_chk_old.csv"
const currFileSaver = "saver_chk_curr.csv"
const tmpFileSaver = "saver_chk_tmp.csv"

func (s *SaverForEx3) DoCheckpoint(responses chan error, id int, chkId int) {
	checkpointer.DoCheckpointWithParser(responses, id, s, "", tmpFileSaver, chkId)
}

func (s *SaverForEx3) RestoreCheckpoint(checkpointToRestore int, id int, responses chan error) {
	checkpointIds := checkpointer.GetCurrentValidCheckpoints(id, "", currFileSaver, oldFileSaver)
	filesArray := []string{
		fmt.Sprintf("%v_%v_%v", id, "", oldFileSaver),
		fmt.Sprintf("%v_%v_%v", id, "", currFileSaver),
	}
	for idx, chkId := range checkpointIds {
		if chkId == checkpointToRestore {
			s.readCheckpointAsState(filesArray[idx])
			break
		}
	}
	responses <- nil
}

func (s *SaverForEx3) GetCheckpointVersions(id int) [2]int {
	return checkpointer.GetCurrentValidCheckpoints(id, "", currFileSaver, oldFileSaver)
}

func (s *SaverForEx3) Commit(id int, responses chan error) {
	log.Debugf("Saver %v | Commiting checkpoint for id: %v", s.id, id)
	checkpointer.HandleOldFile(id, "", oldFileSaver)
	checkpointer.HandleCurrFile(id, "", currFileSaver, oldFileSaver)
	checkpointer.HandleTmpFile(id, "", tmpFileSaver, currFileSaver)
	responses <- nil
}

func (s *SaverForEx3) Abort(id int, responses chan error) {
	checkpointer.DeleteTmpFile(id, "", tmpFileSaver)
	responses <- nil
}

func (s *SaverForEx3) GetCheckpointString() string {
	strBuilder := strings.Builder{}
	for clientId, dynMaps := range s.regsToPersistByClient {
		for journey, dynMapArray := range dynMaps {
			for _, dynamicMap := range dynMapArray {
				if dynamicMap != nil {
					dynMapString := serializer.SerializeToString(dynamicMap)
					// The dynmap serializer adds \n
					strBuilder.WriteString(fmt.Sprintf("%v;%v@%v", clientId, journey, dynMapString))
				}
			}
		}
	}
	return strBuilder.String()
}

func (s *SaverForEx3) readCheckpointAsState(fileToRestore string) {
	if !filemanager.DirectoryExists(fileToRestore) {
		log.Infof("Saver %v | Does not have a checkpoint: %v", s.id, fileToRestore)
		return
	}
	log.Infof("Saver %v | Restoring checkpoint: %v", s.id, fileToRestore)
	fileReader, err := filemanager.NewFileReader(fileToRestore)
	if err != nil {
		log.Fatalf("Saver %v | Error trying to read checkpoint file: %v | %v", s.id, fileToRestore, err)
	}
	defer utils.CloseFileAndNotifyError(fileReader)

	filemanager.SkipHeader(fileReader)
	for fileReader.CanRead() {
		line := fileReader.ReadLine()
		dynMap := dataStructures.NewDynamicMap(make(map[string][]byte))
		keysAndDynMap := strings.Split(line, utils.AtSeparator)
		columns := strings.Split(keysAndDynMap[1], utils.CommaSeparator)
		const columnName = 0
		const columnData = 1
		for _, keyVal := range columns {
			data := strings.Split(keyVal, utils.EqualsSeparator)
			if data[columnName] == utils.TotalStopovers || data[columnName] == utils.ConvertedTravelDuration {
				intVal, err := strconv.Atoi(data[columnData])
				if err != nil {
					log.Errorf("Saver %v | Error converting dynmap column into int | %v", s.id, err)
					continue
				}
				dynMap.AddColumn(data[0], serializer.SerializeUint(uint32(intVal)))
				continue
			}
			if data[columnName] == utils.TotalFare || data[columnName] == utils.TotalTravelDistance {
				floatVal, err := strconv.ParseFloat(data[columnData], 32)
				if err != nil {
					log.Errorf("Saver %v | Error converting dynmap column into float32 | %v", s.id, err)
					continue
				}
				dynMap.AddColumn(data[columnName], serializer.SerializeFloat(float32(floatVal)))
				continue
			}
			dynMap.AddColumn(data[columnName], serializer.SerializeString(data[columnData]))
		}

		clientIdAndJourney := strings.Split(keysAndDynMap[0], utils.DotCommaSeparator)
		if len(clientIdAndJourney) != 2 {
			log.Errorf("Saver %v | Error wrong format in file %v | %v", s.id, fileToRestore, clientIdAndJourney)
			continue
		}
		clientId := clientIdAndJourney[0]
		journey := clientIdAndJourney[1]
		_, exists := s.regsToPersistByClient[clientId]
		if !exists {
			s.regsToPersistByClient[clientId] = make(map[string][2]*dataStructures.DynamicMap)
			s.regsToPersistByClient[clientId][journey] = [2]*dataStructures.DynamicMap{}
		}
		firstDM := s.regsToPersistByClient[clientId][journey][0]
		if firstDM == nil {
			s.regsToPersistByClient[clientId][journey] = [2]*dataStructures.DynamicMap{dynMap}
			continue
		}
		s.regsToPersistByClient[clientId][journey] = [2]*dataStructures.DynamicMap{s.regsToPersistByClient[clientId][journey][0], dynMap}
	}

	err = fileReader.Err()
	if err != nil {
		log.Errorf("Saver %v | Error reading from checkpoint: %v | %v", s.id, fileToRestore, err)
	}
	log.Infof("Saver %v | Restored checkpoint successfully: %v | State recovered: %v", s.id, fileToRestore, s.regsToPersistByClient)
}
