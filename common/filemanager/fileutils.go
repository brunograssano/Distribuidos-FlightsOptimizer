package filemanager

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
)

func MoveFiles(files []string, folderName string) error {

	if _, err := os.Stat(folderName); os.IsNotExist(err) {
		err := os.Mkdir(folderName, os.ModePerm)
		if err != nil && !os.IsExist(err) {
			log.Errorf("FileMover | Error creating directory %v | %v", folderName, err)
			return err
		}
	}
	for _, file := range files {
		err := os.Rename(file, fmt.Sprintf("%v/%v", folderName, file))
		if err != nil {
			log.Errorf("FileMover | Error moving file to '%v/%v' | %v", folderName, file, err)
			return err
		}
	}
	return nil
}

func RenameFile(file string, newName string) error {
	err := os.Rename(file, newName)
	if err != nil {
		log.Errorf("FileRenamer | Error renaming file | %v", err)
		return err
	}
	return nil
}

func DirectoryExists(file string) bool {
	if _, err := os.Stat(file); errors.Is(err, os.ErrNotExist) {
		return false
	}
	return true
}
