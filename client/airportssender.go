package main

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"gopkg.in/errgo.v2/fmt/errors"
	"strings"
)

const AirportFileColumnCount = 11
const AirportCodePos = 0
const LatitudePos = 5
const LongitudePos = 6

// airportLineToDynMap converts a row from the airports file to a dynamic map
func airportLineToDynMap(line string) (*dataStructures.DynamicMap, error) {
	serializer := dataStructures.Serializer{}
	fields := strings.Split(line, utils.CommaSeparator)
	dynMap := &dataStructures.DynamicMap{}
	if len(fields) != AirportFileColumnCount {
		return nil, errors.New("Airports file has incorrect format")
	}
	dynMap.AddColumn(utils.AirportCode, serializer.SerializeString(fields[AirportCodePos]))
	dynMap.AddColumn(utils.Latitude, serializer.SerializeString(fields[LatitudePos]))
	dynMap.AddColumn(utils.Longitude, serializer.SerializeString(fields[LongitudePos]))
	return dynMap, nil
}

// skipHeader Reads a line to skip the header
func skipHeader(reader *filemanager.FileReader) {
	if reader.CanRead() {
		_ = reader.ReadLine()
	}
}

// SendAirports Sends the airports data to the server
func SendAirports(AirportFileName string, batchSize uint, conn *protocol.SocketProtocolHandler) error {
	reader, err := filemanager.NewFileReader(AirportFileName)
	if err != nil {
		return err
	}
	defer utils.CloseFileAndNotifyError(reader)

	rows := make([]*dataStructures.DynamicMap, 0, batchSize)
	addedToMsg := uint(0)

	skipHeader(reader)
	for reader.CanRead() {
		line := reader.ReadLine()
		if addedToMsg >= batchSize {
			msg := &dataStructures.Message{TypeMessage: dataStructures.Airports, DynMaps: rows}
			err = conn.Write(msg)
			if err != nil {
				log.Errorf("%v", err)
				return err
			}
			addedToMsg = 0
			rows = make([]*dataStructures.DynamicMap, 0, batchSize)
		}
		dynMap, err := airportLineToDynMap(line)
		if err != nil {
			log.Errorf("Skipping line: %v", err)
			continue
		}
		rows = append(rows, dynMap)
		addedToMsg++
	}
	err = reader.Err()
	if err != nil {
		log.Errorf("%v", err)
		return err
	}

	return conn.Write(&dataStructures.Message{TypeMessage: dataStructures.EOFAirports})
}
