package checkpointer

import (
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	log "github.com/sirupsen/logrus"
)

func CopyOldIntoCurrent(oldFile string, currentFile string) {
	if !filemanager.DirectoryExists(oldFile) {
		return
	}
	err := filemanager.DeleteFile(currentFile)
	if err != nil {
		log.Errorf("ProducerQueueProtocolHandler | Error deleting current file of checkpointing | %v", err)
	}
	err = filemanager.CopyFile(oldFile, currentFile)
	if err != nil {
		log.Errorf("ProducerQueueProtocolHandler | Error copying file of checkpointing | %v", err)
	}
}
