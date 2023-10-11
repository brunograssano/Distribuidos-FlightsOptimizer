package filemanager

import (
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
