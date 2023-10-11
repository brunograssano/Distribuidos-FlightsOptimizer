package filemanager

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

func MoveFiles(files []string) {
	now := time.Now()
	err := os.Mkdir(fmt.Sprintf("request_%v", now), os.ModePerm)
	if err != nil {
		log.Errorf("FileMover | Error creating directory 'request_%v' | %v", now, err)
		return
	}
	for _, file := range files {
		err = os.Rename(file, fmt.Sprintf("request_%v/%v", now, file))
		if err != nil {
			log.Errorf("FileMover | Error moving file to 'request_%v/%v' | %v", now, file, err)
		}
	}

}
