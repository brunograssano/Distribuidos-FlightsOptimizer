package main

import (
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	log "github.com/sirupsen/logrus"
)

type Getter struct {
	c      *SaverConfig
	server *communication.PassiveTCPSocket
}

func NewGetter(c *SaverConfig) (*Getter, error) {
	server, err := communication.NewPassiveTCPSocket(c.GetterAddress)
	if err != nil {
		log.Errorf("action: create_server | result: error | id: %v | address: %v | %v", c.ID, c.GetterAddress, err)
		return nil, err
	}
	return &Getter{c: c, server: server}, nil
}

func (g *Getter) ReturnResults() {
	defer g.Close()
	for {
		socket, err := g.server.Accept()
		if err != nil {
			log.Errorf("action: accept_connection | result: error | id: %v | address: %v | %v", g.c.ID, g.c.GetterAddress, err)
			return
		}
		g.sendResults(socket)
	}
}

func (g *Getter) sendResults(socket *communication.TCPSocket) {
	defer closeSocket(socket)
	reader, err := filemanager.NewFileReader(g.c.OutputFileName)
	if err != nil {
		return
	}
	defer closeFile(reader)
	// TODO terminar en caso de señal
	for reader.CanRead() {
		line := reader.ReadLine()
		_, err = socket.Write([]byte(line))
		if err != nil {
			log.Errorf("action: sending_file | status: error | %v", err)
			return
		}
	}
	// TODO avisar que se termino el archivo
	err = reader.Err()
	if err != nil {
		log.Errorf("action: read_file | status: error | %v", err)
	}

}

func closeSocket(s communication.TCPSocketInterface) {
	err := s.Close()
	if err != nil {
		log.Errorf("action: closing_socket | status: error | %v", err)
	}
}

func (g *Getter) Close() {
	err := g.server.Close()
	if err != nil {
		log.Errorf("action: close_server | result: error | id: %v | address: %v | %v", g.c.ID, g.c.GetterAddress, err)
	}
}
