package filemanager

import (
	log "github.com/sirupsen/logrus"
	"os"
)

type FileManager struct {
	file     *os.File
	filename string
}

// Close Closes the file
func (f FileManager) Close() error {
	log.Debugf("FileManager | action: closing_file | file_name: %v", f.filename)
	return f.file.Close()
}
