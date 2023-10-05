package parsers

import (
	"fmt"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	"strconv"
	"strings"
)

const airportFileColumnCount = 11
const airportCodePos = 0
const latitudePos = 5
const longitudePos = 6

type AirportsParser struct{}

// LineToDynMap converts a row from the airports file to a dynamic map
func (a AirportsParser) LineToDynMap(line string) (*dataStructures.DynamicMap, error) {
	serializer := dataStructures.Serializer{}
	fields := strings.Split(line, utils.CommaSeparator)
	dynMap := &dataStructures.DynamicMap{}
	if len(fields) != airportFileColumnCount {
		return nil, fmt.Errorf("airports file has incorrect format: %v columns", len(fields))
	}
	dynMap.AddColumn(utils.AirportCode, serializer.SerializeString(fields[airportCodePos]))
	latitude, err := strconv.ParseFloat(fields[latitudePos], 32)
	if err != nil {
		return nil, fmt.Errorf("latitude conversion to float: %v", err)
	}
	dynMap.AddColumn(utils.Latitude, serializer.SerializeFloat(float32(latitude)))

	longitude, err := strconv.ParseFloat(fields[longitudePos], 32)
	if err != nil {
		return nil, fmt.Errorf("longitude conversion to float: %v", err)
	}
	dynMap.AddColumn(utils.Longitude, serializer.SerializeFloat(float32(longitude)))
	return dynMap, nil
}

func (a AirportsParser) GetEofMsgType() int {
	return dataStructures.EOFAirports
}

func (a AirportsParser) GetMsgType() int {
	return dataStructures.Airports
}
