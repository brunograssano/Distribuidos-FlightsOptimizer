package queues

import (
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

func readCheckpointAsState(fileToRestore string, clientsData map[string]int) {
	if !filemanager.DirectoryExists(fileToRestore) {
		log.Infof("ProtocolRecover | Does not have a checkpoint: %v", fileToRestore)
		return
	}
	log.Infof("ProtocolRecover | Restoring checkpoint: %v", fileToRestore)
	fileReader, err := filemanager.NewFileReader(fileToRestore)
	if err != nil {
		log.Fatalf("ProtocolRecover | Error trying to read checkpoint file: %v | %v", fileToRestore, err)
	}
	defer utils.CloseFileAndNotifyError(fileReader)
	filemanager.SkipHeader(fileReader)
	for fileReader.CanRead() {
		line := fileReader.ReadLineAsBytes()
		line = line[:len(line)-1]
		clientIdAndSent := strings.Split(string(line), "=")
		clientId := clientIdAndSent[0]
		sent, err := strconv.Atoi(clientIdAndSent[1])
		if err != nil {
			log.Errorf("ProtocolRecover | On Checkpointing %v | Error trying to convert sent to int: %v | %v", fileToRestore, sent, err)
		}
		clientsData[clientId] = sent
	}
	err = fileReader.Err()
	if err != nil {
		log.Errorf("ProtocolRecover | Error reading from checkpoint: %v | %v", fileToRestore, err)
	}
	log.Infof("ProtocolRecover | Restored checkpoint successfully: %v | State recovered: %v", fileToRestore, clientsData)
}
