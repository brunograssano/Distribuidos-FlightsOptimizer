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

func GetFileToRead(typeOfRecovery CheckpointType, id int, name string, oldFile string, currFile string, tmpFile string) string {
	if typeOfRecovery == Curr {
		currFileName := fmt.Sprintf("%v_%v_%v", id, name, currFile)
		if filemanager.DirectoryExists(currFileName) {
			log.Infof("CheckpointFileManager | Restoring OLD file | Old and Current exist but TMP does not | Type of Recovery: CURRENT")
			return currFileName
		}
		log.Warnf("CheckpointFileManager | No file to restore | Current does not exist | Type of Recovery: CURRENT")
		return ""
	}
	return getOldestCheckpointFileToRead(id, name, oldFile, currFile, tmpFile)
}

func getOldestCheckpointFileToRead(id int, name string, oldFile string, currFile string, tmpFile string) string {
	oldFileName := fmt.Sprintf("%v_%v_%v", id, name, oldFile)
	currFileName := fmt.Sprintf("%v_%v_%v", id, name, currFile)
	tmpFileName := fmt.Sprintf("%v_%v_%v", id, name, tmpFile)
	oldExists := filemanager.DirectoryExists(oldFileName)
	currExists := filemanager.DirectoryExists(currFileName)
	tmpExists := filemanager.DirectoryExists(tmpFileName)
	if oldExists && currExists && tmpExists {
		log.Infof("CheckpointFileManager | Restoring CURRENT file | All 3 files exist | Type of Recovery: OLD")
		// Written TMP with data but could not commit nor abort.
		return currFileName
	}
	if !currExists {
		// Was about to rename TMP into current but couldn't. Last usable checkpoint is OLD.
		log.Warnf("CheckpointFileManager | Current File Name to checkpoint does not exist | %v", currFileName)
		log.Infof("CheckpointFileManager | Restoring OLD file | Current does not exist | Type of Recovery: OLD")
		return oldFileName
	}
	if !oldExists {
		//Was about to rename current to old but crashed
		log.Infof("CheckpointFileManager | Restoring CURRENT file | Old file does not exists | Type of Recovery: OLD")
		return currFileName
	}
	//TMP was already renamed. Checkpoint finished but got OLD, so have to recover the previous commited (OLD).
	log.Infof("CheckpointFileManager | Restoring OLD file | Old and Current exist but TMP does not | Type of Recovery: OLD")
	return oldFileName
}
