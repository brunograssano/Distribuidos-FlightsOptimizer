package getters

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	socketsProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/sockets"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

type ClientGetter struct {
	config   *GetterConfig
	sph      *socketsProtocol.SocketProtocolHandler
	clientId string
	stop     chan bool
	join     chan bool
}

func NewClientGetter(clientSocket *communication.TCPSocket, config *GetterConfig, stop chan bool, join chan bool) *ClientGetter {
	return &ClientGetter{
		config:   config,
		sph:      socketsProtocol.NewSocketProtocolHandler(clientSocket),
		clientId: "",
		stop:     stop,
		join:     join,
	}
}

func (c *ClientGetter) HandleClientGetter() {
	msg, err := c.sph.Read()
	if err != nil {
		log.Errorf("Client Getter | Error getting first message with client id")
		return
	}
	defer c.sph.Close()

	c.clientId = msg.ClientId
	if filemanager.DirectoryExists(msg.ClientId) {
		rowNum, err := msg.DynMaps[0].GetAsInt(utils.NumberOfRow)
		if err != nil {
			log.Errorf("Client Getter | Error trying to get number of row as int from message | %v", err)
		}
		c.sendResults(uint(rowNum))
	} else {
		c.askLaterForResults()
	}
	c.join <- true
	close(c.join)
}

// askLaterForResults Tells the client to wait and finishes the connection
func (c *ClientGetter) askLaterForResults() {
	log.Infof("Client Getter %v | Client asked for results when they are not ready. Answer 'Later'", c.clientId)
	err := c.sph.Write(&dataStructures.Message{
		TypeMessage: dataStructures.Later,
		DynMaps:     make([]*dataStructures.DynamicMap, 0),
	})
	if err != nil {
		log.Errorf("Client Getter %v | Error trying to send 'Later' Message to socket...", c.clientId)
	}
}

// sendResults Sends the saved results to the client
func (c *ClientGetter) sendResults(rowNum uint) {
	log.Infof("Client Getter %v | Sending results to client", c.clientId)
	var currBatch []*dataStructures.DynamicMap
	curLengthOfBatch := 0
	currRowNum := uint(0)
	for _, filename := range c.config.FileNames {
		reader, err := filemanager.NewFileReader(fmt.Sprintf("%v/%v_%v.csv", c.clientId, filename, c.clientId))
		if err != nil {
			log.Errorf("Client Getter %v | Error trying to open file: %v | %v | Skipping it...", c.clientId, filename, err)
			continue
		}
		for reader.CanRead() {
			select {
			case <-c.stop:
				log.Warnf("Client Getter %v | Received signal while sending file, stopping transfer", c.clientId)
				return
			default:
			}
			if currRowNum < rowNum {
				currRowNum++
				continue
			}
			line := reader.ReadLine()
			currBatch = append(currBatch, serializer.DeserializeFromString(line))
			curLengthOfBatch++
			if uint(curLengthOfBatch) >= c.config.MaxLinesPerSend {
				c.sendBatch(currBatch)
				currBatch = make([]*dataStructures.DynamicMap, 0)
			}
		}
		err = reader.Err()
		if err != nil {
			log.Errorf("Client Getter %v| Error reading file | %v", c.clientId, err)
		}
		utils.CloseFileAndNotifyError(reader.FileManager)
	}
	if curLengthOfBatch > 0 {
		c.sendBatch(currBatch)
	}
	c.sendEOF()
}

func (c *ClientGetter) sendEOF() {
	log.Infof("Client Getter %v | Sending EOF to client...", c.clientId)
	err := c.sph.Write(&dataStructures.Message{
		TypeMessage: dataStructures.EOFGetter,
		DynMaps:     []*dataStructures.DynamicMap{},
	})
	if err != nil {
		log.Errorf("Client Getter %v | Error trying to send EOF | %v", c.clientId, err)
	}
}

// sendBatch sends a specific batch of DynamicMaps via protocol handler
func (c *ClientGetter) sendBatch(batch []*dataStructures.DynamicMap) {
	err := c.sph.Write(&dataStructures.Message{
		TypeMessage: dataStructures.FlightRows,
		DynMaps:     batch,
	})
	if err != nil {
		log.Errorf("Client Getter %v | Error sending batch from getter | %v", c.clientId, err)
	}
}
