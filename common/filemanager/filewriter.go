package filemanager

import (
	log "github.com/sirupsen/logrus"
	"os"
)

type FileWriter struct {
	FileManager
}

// NewFileWriter Creates a writer for a file.
// If the file already exists it will append the new content
func NewFileWriter(filename string) (*FileWriter, error) {
	const openFileInWriteModePerm = 0644
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, openFileInWriteModePerm)
	if err != nil {
		log.Errorf("FileWriter | action: open_file | result: fail | file_name: %v | error: %v", filename, err)
		return nil, err
	}
	writer := &FileWriter{
		FileManager: FileManager{file: f, filename: filename},
	}
	return writer, nil
}

// WriteLine Writes a line to the file
func (f *FileWriter) WriteLine(line string) error {
	_, err := f.file.WriteString(line)
	return err
}
