package main

import (
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	log "github.com/sirupsen/logrus"
)

type Getter struct {
	c       *SaverConfig
	server  *communication.PassiveTCPSocket
	stop    chan bool
	canSend chan bool
}

func NewGetter(c *SaverConfig, canSend chan bool) (*Getter, error) {
	server, err := communication.NewPassiveTCPSocket(c.GetterAddress)
	if err != nil {
		log.Errorf("action: create_server | result: error | id: %v | address: %v | %v", c.ID, c.GetterAddress, err)
		return nil, err
	}
	return &Getter{c: c, server: server, stop: make(chan bool), canSend: canSend}, nil
}

func (g *Getter) ReturnResults() {
	defer dataStructures.CloseSocketAndNotifyError(g.server)
	for {
		socket, err := g.server.Accept()
		if err != nil {
			log.Errorf("action: accept_connection | result: error | id: %v | address: %v | %v", g.c.ID, g.c.GetterAddress, err)
			return
		}
		clientSerializer := dataStructures.NewResultsSerializer(socket)
		select {
		case <-g.canSend:
			g.sendResults(clientSerializer)
		default:
			g.askLaterForResults(clientSerializer)
		}

	}
}

func (g *Getter) askLaterForResults(resultsSerializer *dataStructures.ResultsSerializer) {
	resultsSerializer.AskLaterForResults()
	resultsSerializer.Close()
}

func (g *Getter) sendResults(resultsSerializer *dataStructures.ResultsSerializer) {
	defer resultsSerializer.Close()
	reader, err := filemanager.NewFileReader(g.c.OutputFileName)
	if err != nil {
		return
	}
	defer closeFile(reader.FileManager)
	for reader.CanRead() {
		select {
		case <-g.stop:
			log.Warnf("Received signal while sending file, stopping transfer")
			return
		default:
		}
		line := reader.ReadLine()
		err = resultsSerializer.SendLine(line)
		if err != nil {
			log.Errorf("action: sending_file | status: error | %v", err)
			return
		}
	}
	resultsSerializer.EndedFile()
	err = reader.Err()
	if err != nil {
		log.Errorf("action: read_file | status: error | %v", err)
	}

}

func (g *Getter) Close() {
	g.stop <- true
	close(g.stop)
	dataStructures.CloseSocketAndNotifyError(g.server)
}
