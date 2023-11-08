package getters

import (
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

// Getter Server that waits for clients asking for the pipeline results
type Getter struct {
	c            *GetterConfig
	server       *communication.PassiveTCPSocket
	stop         chan bool
	joinChannels []chan bool
	stopChannels []chan bool
}

// NewGetter Creates a new results getter server
func NewGetter(getterConf *GetterConfig) (*Getter, error) {
	server, err := communication.NewPassiveTCPSocket(getterConf.Address)
	if err != nil {
		log.Errorf("Getter | action: create_server | result: error | id: %v | address: %v | %v", getterConf.ID, getterConf.Address, err)
		return nil, err
	}
	return &Getter{c: getterConf, server: server, stop: make(chan bool, 1), joinChannels: []chan bool{}, stopChannels: []chan bool{}}, nil
}

func (g *Getter) ReturnResults() {
	defer utils.CloseSocketAndNotifyError(g.server)
	defer log.Infof("Getter | Finishing Return Loop...")
	for {
		socket, err := g.server.Accept()
		if err != nil {
			log.Errorf("Getter | action: accept_connection | result: error | id: %v | address: %v | %v", g.c.ID, g.c.Address, err)
			return
		}
		stopChannel := make(chan bool, 1)
		joinChannel := make(chan bool, 1)
		g.joinChannels = append(g.joinChannels, joinChannel)
		g.stopChannels = append(g.stopChannels, stopChannel)
		client := NewClientGetter(socket, g.c, joinChannel, stopChannel)
		go client.HandleClientGetter()
		g.clearChannels()
	}
}

func (g *Getter) clearChannels() {
	var indexesToClean []int
	for i := 0; i < len(g.joinChannels); i++ {
		select {
		case <-g.joinChannels[i]:
			indexesToClean = append(indexesToClean, i)
			close(g.stopChannels[i])
		default:
		}
	}
	// Clear in inverse order so that indexes are not moved from array when removing
	for i := len(indexesToClean) - 1; i >= 0; i-- {
		g.joinChannels = append(g.joinChannels[:indexesToClean[i]], g.joinChannels[indexesToClean[i]+1:]...)
		g.stopChannels = append(g.stopChannels[:indexesToClean[i]], g.stopChannels[indexesToClean[i]+1:]...)
	}
}

// Close Stops the execution of the getter server
func (g *Getter) Close() {
	log.Infof("Getter | Sending signal to stop...")
	g.stop <- true
	log.Infof("Getter | Closing stop channel...")
	close(g.stop)
	log.Infof("Getter | Closing server socket...")
	utils.CloseSocketAndNotifyError(g.server)
	log.Infof("Getter | Ended closing resources...")
}
