package ex3

import (
	"fmt"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	queueProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

// SaverForEx3 Structure that handles the final results
type SaverForEx3 struct {
	c             *SaverConfig
	consumer      queueProtocol.ConsumerProtocolInterface
	finishSig     chan bool
	regsToPersist map[string][2]*dataStructures.DynamicMap
	id            int
}

// NewSaverForEx3 Creates a new saver for the results
func NewSaverForEx3(
	consumer queueProtocol.ConsumerProtocolInterface,
	c *SaverConfig,
	finishSig chan bool,
	id int,
) *SaverForEx3 {
	return &SaverForEx3{
		c:             c,
		consumer:      consumer,
		finishSig:     finishSig,
		id:            id,
		regsToPersist: make(map[string][2]*dataStructures.DynamicMap),
	}
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
			s.handleEOF()
		} else if msg.TypeMessage == dataStructures.FlightRows {
			s.handleFlightRow(msg.DynMaps[0])
		}

	}
}

func (s *SaverForEx3) handleFlightRow(flightRow *dataStructures.DynamicMap) {
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
	journeyMap, existsJM := s.regsToPersist[journeyStr]
	flightRow.AddColumn(utils.ConvertedTravelDuration, serializer.SerializeUint(uint32(convertedTravelDuration)))
	if existsJM {
		newRows := DecideWhichRowsToKeep(journeyMap, flightRow, s.id)
		s.regsToPersist[journeyStr] = newRows
	} else {
		s.regsToPersist[journeyStr] = [2]*dataStructures.DynamicMap{flightRow, nil}
	}

}

func (s *SaverForEx3) handleEOF() {
	log.Infof("Saver %v | Received all results | Persisting to file...", s.id)
	s.persistToFile()
	s.regsToPersist = make(map[string][2]*dataStructures.DynamicMap)
	log.Infof("Saver %v | Sending finish signal...", s.id)
	s.finishSig <- true
}

func (s *SaverForEx3) persistToFile() {
	writer, err := filemanager.NewFileWriter(fmt.Sprintf("%v_%v.csv", s.c.OutputFilePrefix, s.id))
	if err != nil {
		log.Errorf("Saver %v | Error opening file | %v | Closing saver", s.id, err)
		return
	}
	for journey, rows := range s.regsToPersist {
		line := fmt.Sprintf("journey=%v\n", journey)
		err := writer.WriteLine(line)
		if err != nil {
			log.Errorf("Saver %v | Error writing journey into file | %v | Skipping it...", s.id, err)
			continue
		}
		for _, row := range rows {
			if row != nil {
				line := serializer.SerializeToString(row)
				err = writer.WriteLine(line)
				if err != nil {
					log.Errorf("Saver %v | Error writing journey's row into file | %v | Skipping this row...", s.id, err)
					continue
				}
			}
		}
	}
	utils.CloseFileAndNotifyError(writer.FileManager)
}
