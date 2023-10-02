package data_structures

import (
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

type ResultsSerializer struct {
	socket communication.TCPSocketInterface
}

func NewResultsSerializer(socket communication.TCPSocketInterface) *ResultsSerializer {
	return &ResultsSerializer{socket: socket}
}

func (r *ResultsSerializer) SendLine(line string) error {
	_, err := r.socket.Write([]byte(line))
	return err
}

func (r *ResultsSerializer) Close() {
	utils.CloseSocketAndNotifyError(r.socket)
}

func (r *ResultsSerializer) AskLaterForResults() {
	log.Infof("action: sending_later_msg ")
	_, err := r.socket.Write([]byte("LATER"))
	if err != nil {
		log.Errorf("action: sending_later_msg | status: error | %v", err)
	}
}

func (r *ResultsSerializer) EndedFile() {
	log.Infof("action: sending_end_msg ")
	_, err := r.socket.Write([]byte("ENDED_FILE"))
	if err != nil {
		log.Errorf("action: sending_ended_msg | status: error | %v", err)
	}
}
