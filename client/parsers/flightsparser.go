package parsers

import (
	"fmt"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

const flightFileColumnCount = 27

const legIdPos = 0
const startingAirportPos = 3
const destinationAirportPos = 4
const travelDurationPos = 6
const totalFarePos = 12
const totalTravelDistancePos = 14
const segmentsArrivalAirportCodePos = 19
const segmentsAirlineNamePos = 21

type FlightsParser struct{}

// LineToDynMap Parses a line from the flights file
func (a FlightsParser) LineToDynMap(line string) (*dataStructures.DynamicMap, error) {
	serializer := dataStructures.Serializer{}
	fields := strings.Split(line, utils.CommaSeparator)
	dynMap := dataStructures.NewDynamicMap(make(map[string][]byte))
	if len(fields) != flightFileColumnCount {
		return nil, fmt.Errorf("flights file has incorrect format: %v columns", len(fields))
	}
	dynMap.AddColumn(utils.LegId, serializer.SerializeString(fields[legIdPos]))
	dynMap.AddColumn(utils.StartingAirport, serializer.SerializeString(fields[startingAirportPos]))
	dynMap.AddColumn(utils.DestinationAirport, serializer.SerializeString(fields[destinationAirportPos]))
	dynMap.AddColumn(utils.TravelDuration, []byte(fields[travelDurationPos]))

	totalFare, err := strconv.ParseFloat(fields[totalFarePos], 32)
	if err != nil {
		return nil, fmt.Errorf("totalFare conversion to float: %v", err)
	}
	dynMap.AddColumn(utils.TotalFare, serializer.SerializeFloat(float32(totalFare)))
	totalTravelDistance := float64(0)
	if fields[totalTravelDistancePos] != "" {
		totalTravelDistance, err = strconv.ParseFloat(fields[totalTravelDistancePos], 32)
		if err != nil {
			log.Warnf("FlightsParser | Error converting totalTravelDistance | %v | Will be sent as zero", err)
		}
	}

	dynMap.AddColumn(utils.TotalTravelDistance, serializer.SerializeFloat(float32(totalTravelDistance)))
	dynMap.AddColumn(utils.SegmentsArrivalAirportCode, serializer.SerializeString(fields[segmentsArrivalAirportCodePos]))
	dynMap.AddColumn(utils.SegmentsAirlineName, serializer.SerializeString(fields[segmentsAirlineNamePos]))

	return dynMap, nil
}

func (a FlightsParser) GetEofMsgType() int {
	return dataStructures.EOFFlightRows
}

func (a FlightsParser) GetMsgType() int {
	return dataStructures.FlightRows
}
