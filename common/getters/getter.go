package getters

import (
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

// Getter Server that waits for clients asking for the pipeline results
type Getter struct {
	c       *GetterConfig
	server  *communication.PassiveTCPSocket
	stop    chan bool
	canSend chan bool
}

// NewGetter Creates a new results getter server
func NewGetter(getterConf *GetterConfig, canSend chan bool) (*Getter, error) {
	server, err := communication.NewPassiveTCPSocket(getterConf.Address)
	if err != nil {
		log.Errorf("action: create_server | result: error | id: %v | address: %v | %v", getterConf.ID, getterConf.Address, err)
		return nil, err
	}
	return &Getter{c: getterConf, server: server, stop: make(chan bool), canSend: canSend}, nil
}

func (g *Getter) ReturnResults() {
	defer utils.CloseSocketAndNotifyError(g.server)
	for {
		socket, err := g.server.Accept()
		if err != nil {
			log.Errorf("action: accept_connection | result: error | id: %v | address: %v | %v", g.c.ID, g.c.Address, err)
			return
		}
		sph := protocol.NewSocketProtocolHandler(socket)
		select {
		case <-g.canSend:
			g.sendResults(sph)
		default:
			g.askLaterForResults(sph)
		}
		sph.Close()
	}
}

// askLaterForResults Tells the client to wait and finishes the connection
func (g *Getter) askLaterForResults(sph *protocol.SocketProtocolHandler) {
	err := sph.Write(&dataStructures.Message{
		TypeMessage: dataStructures.Later,
		DynMaps:     make([]*dataStructures.DynamicMap, 0),
	})
	if err != nil {
		log.Errorf("Error trying to send 'Later' Message to socket...")
		return
	}
}

// sendResults Sends the saved results to the client
func (g *Getter) sendResults(sph *protocol.SocketProtocolHandler) {
	var currBatch []*dataStructures.DynamicMap
	curLengthOfBatch := 0
	serializer := dataStructures.NewSerializer()
	for _, filename := range g.c.FileNames {
		reader, err := filemanager.NewFileReader(filename)
		if err != nil {
			log.Errorf("Error trying to open file: %v. Skipping it...", filename)
			continue
		}
		for reader.CanRead() {
			select {
			case <-g.stop:
				log.Warnf("Received signal while sending file, stopping transfer")
				return
			default:
			}
			line := reader.ReadLine()
			currBatch = append(currBatch, serializer.DeserializeFromString(line))
			curLengthOfBatch++
			if uint(curLengthOfBatch) >= g.c.MaxLinesPerSend {
				g.sendBatch(sph, currBatch)
				currBatch = make([]*dataStructures.DynamicMap, 0)
			}
		}
		err = reader.Err()
		if err != nil {
			log.Errorf("action: read_file | status: error | %v", err)
		}
		err = reader.FileManager.Close()
		if err != nil {
			log.Errorf("action: closing_file | status: error | %v", err)
		}
	}
	if curLengthOfBatch > 0 {
		g.sendBatch(sph, currBatch)
	}
	g.sendEOF(sph)
}

func (g *Getter) sendEOF(sph *protocol.SocketProtocolHandler) {
	log.Infof("Sending EOF to client...")
	err := sph.Write(&dataStructures.Message{
		TypeMessage: dataStructures.EOFGetter,
		DynMaps:     []*dataStructures.DynamicMap{},
	})
	if err != nil {
		log.Errorf("Error trying to send EOF: %v", err)
	}
}

// sendBatch sends a specific batch of DynamicMaps via protocol handler
func (g *Getter) sendBatch(sph *protocol.SocketProtocolHandler, batch []*dataStructures.DynamicMap) {
	err := sph.Write(&dataStructures.Message{
		TypeMessage: dataStructures.FlightRows,
		DynMaps:     batch,
	})
	if err != nil {
		log.Errorf("Error sending batch from getter: %v", err)
	}
}

// Close Stops the execution of the getter server
func (g *Getter) Close() {
	g.stop <- true
	close(g.stop)
	utils.CloseSocketAndNotifyError(g.server)
}
