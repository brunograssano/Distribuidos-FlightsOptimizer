package utils

import (
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	log "github.com/sirupsen/logrus"
	"io"
)

// CloseFileAndNotifyError Closes a file and logs an error if any
func CloseFileAndNotifyError(file io.Closer) {
	err := file.Close()
	if err != nil {
		log.Errorf("action: closing_file | status: error | %v", err)
	}
}

// CloseSocketAndNotifyError Closes a socket and logs an error if any
func CloseSocketAndNotifyError(s communication.TCPSocketInterface) {
	err := s.Close()
	if err != nil {
		log.Errorf("action: closing_socket | status: error | %v", err)
	}
}
