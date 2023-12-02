package ex3

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	queueProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"os"
	"strings"
)

// SaverForEx3 Structure that handles the final results
type SaverForEx3 struct {
	c                     *SaverConfig
	consumer              queueProtocol.ConsumerProtocolInterface
	finishSig             chan string
	regsToPersistByClient map[string]map[string][2]*dataStructures.DynamicMap
	id                    int
	checkpointer          *checkpointer.CheckpointerHandler
}

// NewSaverForEx3 Creates a new saver for the results
func NewSaverForEx3(
	consumer queueProtocol.ConsumerProtocolInterface,
	c *SaverConfig,
	finishSig chan string,
	id int,
	chkHandler *checkpointer.CheckpointerHandler,
) *SaverForEx3 {
	chkHandler.AddCheckpointable(consumer, id)
	saver := &SaverForEx3{
		c:                     c,
		consumer:              consumer,
		finishSig:             finishSig,
		id:                    id,
		regsToPersistByClient: make(map[string]map[string][2]*dataStructures.DynamicMap),
		checkpointer:          chkHandler,
	}
	chkHandler.AddCheckpointable(saver, id)
	return saver
}

// SaveData Saves the results from the queue in a file
func (s *SaverForEx3) SaveData() {
	log.Infof("Saver %v | Started goroutine", s.id)
	for {
		msg, ok := s.consumer.Pop()
		if !ok {
			log.Infof("Saver %v | Exiting saver", s.id)
			return
		}

		log.Debugf("Saver %v | Received message: %v", s.id, msg)
		if msg.TypeMessage == dataStructures.EOFFlightRows {
			s.handleEOF(msg.ClientId)
		} else if msg.TypeMessage == dataStructures.FlightRows {
			s.handleFlightRow(msg.DynMaps[0], msg.ClientId)
		}
		err := s.checkpointer.DoCheckpoint(s.id)
		if err != nil {
			log.Errorf("Saver %v | Error on checkpointing | %v", s.id, err)
		}
	}
}

func (s *SaverForEx3) handleFlightRow(flightRow *dataStructures.DynamicMap, clientId string) {
	travelDurationStr, err := flightRow.GetAsString(utils.TravelDuration)
	if err != nil {
		log.Errorf("Saver %v | Error trying to get travel duration string | %v", s.id, err)
	}
	stAirport, err := flightRow.GetAsString(utils.StartingAirport)
	if err != nil {
		log.Errorf("Saver %v | Error trying to get starting airport string | %v", s.id, err)
	}
	destAirport, err := flightRow.GetAsString(utils.DestinationAirport)
	if err != nil {
		log.Errorf("Saver %v | Error trying to get destination airport string | %v", s.id, err)
	}
	convertedTravelDuration, err := utils.ConvertTravelDurationToMinutesAsInt(travelDurationStr)
	if err != nil {
		log.Errorf("Saver %v | Error trying to convert travel duration to minutes | %v", s.id, err)
	}
	journeyStr := fmt.Sprintf("%v-%v", stAirport, destAirport)
	_, existsClient := s.regsToPersistByClient[clientId]
	if !existsClient {
		s.regsToPersistByClient[clientId] = make(map[string][2]*dataStructures.DynamicMap)
	}
	journeyMap, existsJM := s.regsToPersistByClient[clientId][journeyStr]
	flightRow.AddColumn(utils.ConvertedTravelDuration, serializer.SerializeUint(uint32(convertedTravelDuration)))
	if existsJM {
		newRows := DecideWhichRowsToKeep(journeyMap, flightRow, s.id)
		s.regsToPersistByClient[clientId][journeyStr] = newRows
	} else {
		s.regsToPersistByClient[clientId][journeyStr] = [2]*dataStructures.DynamicMap{flightRow, nil}
	}

}

func (s *SaverForEx3) handleEOF(clientId string) {
	log.Infof("Saver %v | Received all results | Persisting to file...", s.id)
	s.persistToFile(clientId)
	s.regsToPersistByClient[clientId] = make(map[string][2]*dataStructures.DynamicMap)
	log.Infof("Saver %v | Sending finish signal...", s.id)
	s.finishSig <- clientId
}

func (s *SaverForEx3) persistToFile(clientId string) {
	fileName := fmt.Sprintf("%v_%v_%v.csv", s.c.OutputFilePrefix, s.id, clientId)
	writer, err := filemanager.NewFileWriter(fileName)
	if err != nil {
		log.Errorf("Saver %v | Error opening file | %v | Closing saver", s.id, err)
		return
	}
	line := strings.Builder{}
	for journey, rows := range s.regsToPersistByClient[clientId] {
		line.WriteString(fmt.Sprintf("journey=%v\n", journey))
		for _, row := range rows {
			if row != nil {
				line.WriteString(serializer.SerializeToString(row))
			}
		}
	}
	err = writer.WriteLine(line.String())
	if err != nil {
		log.Errorf("Saver %v | Error writing journey into file | %v | Skipping it...", s.id, err)
	}
	utils.CloseFileAndNotifyError(writer.FileManager)
	err = filemanager.MoveFiles([]string{fileName}, fmt.Sprintf("%v_tmp", clientId))
	if err != nil {
		log.Errorf("Saver %v | Error moving file | %v | Closing saver", s.id, err)
	}
}
