package filemanager

import (
	"bufio"
	log "github.com/sirupsen/logrus"
	"os"
)

type FileManager struct {
	file     *os.File
	filename string
}

type FileReader struct {
	FileManager
	scanner *bufio.Scanner
}

type FileWriter struct {
	FileManager
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

// Close Closes the file
func (f FileManager) Close() error {
	log.Debugf("FileManager | action: closing_file | file_name: %v", f.filename)
	return f.file.Close()
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

// WriteLine Writes a line to the file
func (f *FileWriter) WriteLine(line string) error {
	_, err := f.file.WriteString(line)
	return err
}
