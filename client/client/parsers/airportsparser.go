package parsers

import (
	"fmt"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	"strconv"
	"strings"
)

const airportCodePos = 0
const airportName = 1
const cityName = 2
const countryName = 3
const countryCode = 4
const latitudePos = 5
const longitudePos = 6
const worldAreaCode = 7
const cityNameId = 8
const countryNameId = 9
const coordinates = 10
const airportFileColumnCount = 11

type AirportsParser struct{}

// LineToDynMap converts a row from the airports file to a dynamic map
func (a AirportsParser) LineToDynMap(line string) (*dataStructures.DynamicMap, error) {
	fields := strings.Split(line, utils.DotCommaSeparator)
	dynMap := dataStructures.NewDynamicMap(make(map[string][]byte))
	if len(fields) != airportFileColumnCount {
		return nil, fmt.Errorf("airports file has incorrect format: %v columns", len(fields))
	}
	dynMap.AddColumn(utils.AirportCode, serializer.SerializeString(fields[airportCodePos]))
	dynMap.AddColumn(utils.AirportName, serializer.SerializeString(fields[airportName]))
	dynMap.AddColumn(utils.CityName, serializer.SerializeString(fields[cityName]))
	dynMap.AddColumn(utils.CountryName, serializer.SerializeString(fields[countryName]))
	dynMap.AddColumn(utils.CountryCode, serializer.SerializeString(fields[countryCode]))
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
	code, err := strconv.ParseUint(fields[worldAreaCode], 10, 32)
	if err != nil {
		code = 0
	}
	dynMap.AddColumn(utils.WorldAreaCode, serializer.SerializeUint(uint32(code)))
	dynMap.AddColumn(utils.CityNameId, serializer.SerializeString(fields[cityNameId]))
	code, err = strconv.ParseUint(fields[countryNameId], 10, 32)
	if err != nil {
		code = 0
	}
	dynMap.AddColumn(utils.CountryNameId, serializer.SerializeUint(uint32(code)))
	dynMap.AddColumn(utils.Coordinates, serializer.SerializeString(fields[coordinates]))

	return dynMap, nil
}

func (a AirportsParser) GetEofMsgType() int {
	return dataStructures.EOFAirports
}

func (a AirportsParser) GetMsgType() int {
	return dataStructures.Airports
}
