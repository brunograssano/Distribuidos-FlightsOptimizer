package main

import (
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

// Getter Server that waits for clients asking for the pipeline results
type Getter struct {
	c       *SaverConfig
	server  *communication.PassiveTCPSocket
	stop    chan bool
	canSend chan bool
}

// NewGetter Creates a new results getter server
func NewGetter(c *SaverConfig, canSend chan bool) (*Getter, error) {
	server, err := communication.NewPassiveTCPSocket(c.GetterAddress)
	if err != nil {
		log.Errorf("action: create_server | result: error | id: %v | address: %v | %v", c.ID, c.GetterAddress, err)
		return nil, err
	}
	return &Getter{c: c, server: server, stop: make(chan bool), canSend: canSend}, nil
}

// ReturnResults Basic server loop to return the results
func (g *Getter) ReturnResults() {
	defer utils.CloseSocketAndNotifyError(g.server)
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

// askLaterForResults Tells the client to wait and finishes the connection
func (g *Getter) askLaterForResults(resultsSerializer *dataStructures.ResultsSerializer) {
	resultsSerializer.AskLaterForResults()
	resultsSerializer.Close()
}

// sendResults Sends the saved results to the client
func (g *Getter) sendResults(resultsSerializer *dataStructures.ResultsSerializer) {
	defer resultsSerializer.Close()
	reader, err := filemanager.NewFileReader(g.c.OutputFileName)
	if err != nil {
		return
	}
	defer utils.CloseFileAndNotifyError(reader.FileManager)
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

// Close Stops the execution of the getter server
func (g *Getter) Close() {
	g.stop <- true
	close(g.stop)
	utils.CloseSocketAndNotifyError(g.server)
}
