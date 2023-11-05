package filemanager

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

func MoveFiles(files []string) (string, error) {
	now := time.Now()
	folder := fmt.Sprintf("request_%v", now)
	err := os.Mkdir(folder, os.ModePerm)
	if err != nil {
		log.Errorf("FileMover | Error creating directory 'request_%v' | %v", now, err)
		return "", err
	}
	for _, file := range files {
		err = os.Rename(file, fmt.Sprintf("request_%v/%v", now, file))
		if err != nil {
			log.Errorf("FileMover | Error moving file to 'request_%v/%v' | %v", now, file, err)
		}
	}
	return folder, nil
}

func RenameFile(file string, newName string) error {
	err := os.Rename(file, newName)
	if err != nil {
		log.Errorf("FileRenamer | Error renaming file | %v", err)
		return err
	}
	return nil
}

func FileExists(file string) bool {
	if _, err := os.Stat(file); errors.Is(err, os.ErrNotExist) {
		return false
	}
	return true
}
