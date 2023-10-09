package main

import (
	"fmt"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

// SaverForEx3 Structure that handles the final results
type SaverForEx3 struct {
	c             *SaverConfig
	consumer      protocol.ConsumerProtocolInterface
	finishSig     chan bool
	regsToPersist map[string][2]*dataStructures.DynamicMap
	id            int
	serializer    *dataStructures.Serializer
}

// NewSaverForEx3 Creates a new saver for the results
func NewSaverForEx3(
	consumer protocol.ConsumerProtocolInterface,
	c *SaverConfig,
	finishSig chan bool,
	id int,
) *SaverForEx3 {
	return &SaverForEx3{
		c:             c,
		consumer:      consumer,
		finishSig:     finishSig,
		id:            id,
		serializer:    dataStructures.NewSerializer(),
		regsToPersist: make(map[string][2]*dataStructures.DynamicMap),
	}
}

// SaveData Saves the results from the queue in a file
func (s *SaverForEx3) SaveData() {
	for {
		msg, ok := s.consumer.Pop()
		if !ok {
			log.Infof("[SAVER %v] Exiting saver", s.id)
			return
		}

		log.Debugf("Received message: %v", msg)
		if msg.TypeMessage == dataStructures.EOFFlightRows {
			s.handleEOF()
			return
		} else if msg.TypeMessage == dataStructures.FlightRows {
			s.handleFlightRow(msg.DynMaps[0])
		}

	}
}

func (s *SaverForEx3) handleFlightRow(flightRow *dataStructures.DynamicMap) {
	travelDurationStr, err := flightRow.GetAsString(utils.TravelDuration)
	if err != nil {
		log.Errorf("[SAVER %v] Error trying to get travel duration string: %v", s.id, err)
	}
	stAirport, err := flightRow.GetAsString(utils.StartingAirport)
	if err != nil {
		log.Errorf("[SAVER %v] Error trying to get starting airport string: %v", s.id, err)
	}
	destAirport, err := flightRow.GetAsString(utils.DestinationAirport)
	if err != nil {
		log.Errorf("[SAVER %v] Error trying to get destination airport string: %v", s.id, err)
	}
	convertedTravelDuration, err := utils.ConvertTravelDurationToMinutesAsInt(travelDurationStr)
	if err != nil {
		log.Errorf("[SAVER %v] Error trying to convert travel duration to minutes: %v", s.id, err)
	}
	journeyStr := fmt.Sprintf("%v-%v", stAirport, destAirport)
	journeyMap, existsJM := s.regsToPersist[journeyStr]
	flightRow.AddColumn(utils.ConvertedTravelDuration, s.serializer.SerializeUint(uint32(convertedTravelDuration)))
	if existsJM {
		newRows := DecideWhichRowsToKeep(journeyMap, flightRow, s.id)
		s.regsToPersist[journeyStr] = newRows
	} else {
		s.regsToPersist[journeyStr] = [2]*dataStructures.DynamicMap{flightRow, nil}
	}

}

func (s *SaverForEx3) handleEOF() {
	log.Infof("[SAVER %v] Received all results. Persisting to file...", s.id)
	s.persistToFile()
	log.Infof("[SAVER %v] Sending finish signal...", s.id)
	s.finishSig <- true
}

func (s *SaverForEx3) persistToFile() {
	writer, err := filemanager.NewFileWriter(fmt.Sprintf("%v_%v.csv", s.c.OutputFilePrefix, s.id))
	if err != nil {
		log.Errorf("[SAVER %v] Error opening file, closing saver", s.id)
		return
	}
	serializer := dataStructures.NewSerializer()
	for journey, rows := range s.regsToPersist {
		line := fmt.Sprintf("journey=%v\n", journey)
		err := writer.WriteLine(line)
		if err != nil {
			log.Errorf("[SAVER %v] Error writing journey into file: %v. Skipping it...", s.id, err)
			continue
		}
		for _, row := range rows {
			if row != nil {
				line := serializer.SerializeToString(row)
				err = writer.WriteLine(line)
				if err != nil {
					log.Errorf("[SAVER %v] Error writing journey's row into file: %v. Skipping this row...", s.id, err)
					continue
				}
			}
		}
	}
	utils.CloseFileAndNotifyError(writer.FileManager)
}
