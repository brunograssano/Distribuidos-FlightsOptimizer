package filemanager

import (
	"bufio"
	log "github.com/sirupsen/logrus"
	"os"
)

type FileReader struct {
	FileManager
	scanner *bufio.Scanner
}

// NewFileReader Creates a new reader of a file.
// The file will be opened in READ ONLY mode
func NewFileReader(filename string) (*FileReader, error) {
	f, err := os.Open(filename)
	if err != nil {
		log.Errorf("FileReader | action: open_file | result: fail | file_name: %v | error: %v", filename, err)
		return nil, err
	}
	scanner := bufio.NewScanner(f)
	log.Debugf("FileReader | action: opened_file | file_name: %v", filename)
	reader := &FileReader{
		FileManager: FileManager{file: f, filename: filename},
		scanner:     scanner,
	}
	return reader, nil
}

// ReadLine Returns the line read
func (f *FileReader) ReadLine() string {
	return f.scanner.Text()
}

// CanRead Returns whether it can read or not
// If it managed to read (returned true), the content can be obtained by calling ReadLine
func (f *FileReader) CanRead() bool {
	return f.scanner.Scan()
}

// Err Returns an error if it was encountered
func (f *FileReader) Err() error {
	return f.scanner.Err()
}
