package checkpointer

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

func DoCheckpointWithParser(errors chan error, id int, writer CheckpointWriter, name string, tmpFile string) {
	tmpFileName := fmt.Sprintf("%v_%v_%v", id, name, tmpFile)
	log.Debugf("CheckpointFileManager | Performing checkpoint: %v", tmpFileName)
	fileWriter, err := filemanager.NewFileWriter(tmpFileName)
	if err != nil {
		log.Errorf("CheckpointFileManager | Error trying to create tmp checkpoint file: %v | %v", tmpFileName, err)
		errors <- err
		return
	}
	defer utils.CloseFileAndNotifyError(fileWriter)
	linesToWrite := writer.GetCheckpointString()
	err = fileWriter.WriteLine(linesToWrite)
	if err != nil {
		log.Errorf("CheckpointFileManager | Error trying to write the checkpoint: %v | %v", tmpFileName, err)
		errors <- err
		return
	}
	errors <- nil
}

func HandleTmpFile(id int, name string, tmpFile string, currFile string) {
	tmpFileName := fmt.Sprintf("%v_%v_%v", id, name, tmpFile)
	currFileName := fmt.Sprintf("%v_%v_%v", id, name, currFile)
	if !filemanager.DirectoryExists(tmpFileName) {
		log.Errorf("CheckpointFileManager | Tmp file %v does not exist", tmpFileName)
		return
	}
	log.Debugf("CheckpointFileManager | Renaming TMP Checkpoint %v into Current %v", tmpFileName, currFileName)
	err := filemanager.RenameFile(tmpFileName, currFileName)
	if err != nil {
		log.Errorf("CheckpointFileManager | Error renaming TMP Checkpoint %v into Current %v | %v", tmpFileName, currFileName, err)
	}
}

func HandleCurrFile(id int, name string, currFile string, oldFile string) {
	currFileName := fmt.Sprintf("%v_%v_%v", id, name, currFile)
	oldFileName := fmt.Sprintf("%v_%v_%v", id, name, oldFile)
	if !filemanager.DirectoryExists(currFileName) {
		log.Debugf("CheckpointFileManager | CurrFile %v does not exist", currFileName)
		return
	}
	log.Debugf("CheckpointFileManager | Renaming Current (%v) into Old Checkpoint (%v)", currFileName, oldFileName)
	err := filemanager.RenameFile(currFileName, oldFileName)
	if err != nil {
		log.Errorf("CheckpointFileManager | Error renaming current checkpoint file: %v | %v", currFileName, err)
	}
}

func HandleOldFile(id int, name string, oldFile string) {
	oldFileName := fmt.Sprintf("%v_%v_%v", id, name, oldFile)
	if !filemanager.DirectoryExists(oldFileName) {
		log.Debugf("CheckpointFileManager | OldFile %v does not exist", oldFileName)
		return
	}
	log.Debugf("CheckpointFileManager | Deleting Old Checkpoint %v", oldFileName)
	err := filemanager.DeleteFile(oldFileName)
	if err != nil {
		log.Errorf("CheckpointFileManager | Error deleting old file | %v", err)
	}
}

func DeleteTmpFile(id int, name string, tmpFile string) {
	tmpFileName := fmt.Sprintf("%v_%v_%v", id, name, tmpFile)
	log.Debugf("CheckpointFileManager | Aborting Checkpoint | Deleting TMP Checkpoint File: %v", tmpFileName)
	err := filemanager.DeleteFile(tmpFileName)
	if err != nil {
		log.Errorf("CheckpointFileManager | Error deleting tmp file | %v", err)
	}
}

func PendingCheckpointExists(id int, name string, tmpFile string) bool {
	return filemanager.DirectoryExists(fmt.Sprintf("%v_%v_%v", id, name, tmpFile))
}

func GetFileToRead(typeOfRecovery CheckpointType, id int, name string, oldFileProd string, currFileProd string, tmpFileProd string) string {
	fileToRead := fmt.Sprintf("%v_%v_%v", id, name, currFileProd)
	if typeOfRecovery == Old {
		fileToRead = getOldestCheckpointFileToRead(id, name, oldFileProd, currFileProd, tmpFileProd)
	}
	return fileToRead
}

func getOldestCheckpointFileToRead(id int, name string, oldFile string, currFile string, tmpFile string) string {
	oldFileName := fmt.Sprintf("%v_%v_%v", id, name, oldFile)
	currFileName := fmt.Sprintf("%v_%v_%v", id, name, currFile)
	tmpFileName := fmt.Sprintf("%v_%v_%v", id, name, tmpFile)
	if !filemanager.DirectoryExists(currFileName) {
		log.Warnf("CheckpointFileManager | Current File Name to checkpoint does not exist | %v", currFileName)
	}
	if PendingCheckpointExists(id, name, tmpFile) {
		CopyOldIntoCurrent(oldFileName, currFileName)
		err := filemanager.DeleteFile(tmpFileName)
		if err != nil {
			log.Errorf("CheckpointFileManager %v | Error deleting TMP file when restoring checkpoint: %v", id, tmpFileName)
		}
	}
	fileToRead := oldFileName
	if !filemanager.DirectoryExists(oldFileName) {
		fileToRead = currFileName
	}
	return fileToRead
}
