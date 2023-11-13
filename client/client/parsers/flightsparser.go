package parsers

import (
	"fmt"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

const flightFileColumnCount = 27

const legIdPos = 0
const searchDatePos = 1
const flightDatePos = 2
const startingAirportPos = 3
const destinationAirportPos = 4
const fareBasisCodePos = 5
const travelDurationPos = 6
const elapsedDaysPos = 7
const isBasicEconomyPos = 8
const isRefundablePos = 9
const isNonStopPos = 10
const baseFarePos = 11
const totalFarePos = 12
const seatsRemainingPos = 13
const totalTravelDistancePos = 14
const segmentsDepartureTimeEpochSecondsPos = 15
const segmentsDepartureTimeRawPos = 16
const segmentsArrivalTimeEpochSecondsPos = 17
const segmentsArrivalTimeRawPos = 18
const segmentsArrivalAirportCodePos = 19
const segmentsDepartureAirportCodePos = 20
const segmentsAirlineNamePos = 21
const segmentsAirlineCodePos = 22
const segmentsEquipmentDescriptionPos = 23
const segmentsDurationInSecondsPos = 24
const segmentsDistancePos = 25
const segmentsCabinCodePos = 26

type FlightsParser struct{}

// LineToDynMap Parses a line from the flights file
func (a FlightsParser) LineToDynMap(line string) (*dataStructures.DynamicMap, error) {
	fields := strings.Split(line, utils.CommaSeparator)
	dynMap := dataStructures.NewDynamicMap(make(map[string][]byte))
	if len(fields) != flightFileColumnCount {
		return nil, fmt.Errorf("flights file has incorrect format: %v columns", len(fields))
	}
	dynMap.AddColumn(utils.LegId, serializer.SerializeString(fields[legIdPos]))
	dynMap.AddColumn(utils.SearchDate, serializer.SerializeString(fields[searchDatePos]))
	dynMap.AddColumn(utils.FlightDate, serializer.SerializeString(fields[flightDatePos]))
	dynMap.AddColumn(utils.StartingAirport, serializer.SerializeString(fields[startingAirportPos]))
	dynMap.AddColumn(utils.DestinationAirport, serializer.SerializeString(fields[destinationAirportPos]))
	dynMap.AddColumn(utils.FareBasisCode, serializer.SerializeString(fields[fareBasisCodePos]))
	dynMap.AddColumn(utils.TravelDuration, serializer.SerializeString(fields[travelDurationPos]))
	dynMap.AddColumn(utils.ElapsedDays, serializer.SerializeString(fields[elapsedDaysPos]))
	dynMap.AddColumn(utils.IsBasicEconomy, serializer.SerializeString(fields[isBasicEconomyPos]))
	dynMap.AddColumn(utils.IsRefundable, serializer.SerializeString(fields[isRefundablePos]))
	dynMap.AddColumn(utils.IsNonStop, serializer.SerializeString(fields[isNonStopPos]))
	dynMap.AddColumn(utils.BaseFare, serializer.SerializeString(fields[baseFarePos]))

	totalFare, err := strconv.ParseFloat(fields[totalFarePos], 32)
	if err != nil {
		return nil, fmt.Errorf("totalFare conversion to float: %v", err)
	}
	dynMap.AddColumn(utils.TotalFare, serializer.SerializeFloat(float32(totalFare)))
	dynMap.AddColumn(utils.SeatsRemaining, serializer.SerializeString(fields[seatsRemainingPos]))
	dynMap.AddColumn(utils.TotalTravelDistance, serializer.SerializeFloat(a.getTotalTravelDistance(fields)))
	dynMap.AddColumn(utils.SegmentsDepartureTimeEpochSeconds, serializer.SerializeString(fields[segmentsDepartureTimeEpochSecondsPos]))
	dynMap.AddColumn(utils.SegmentsDepartureTimeRaw, serializer.SerializeString(fields[segmentsDepartureTimeRawPos]))
	dynMap.AddColumn(utils.SegmentsArrivalTimeEpochSeconds, serializer.SerializeString(fields[segmentsArrivalTimeEpochSecondsPos]))
	dynMap.AddColumn(utils.SegmentsArrivalTimeRaw, serializer.SerializeString(fields[segmentsArrivalTimeRawPos]))
	dynMap.AddColumn(utils.SegmentsArrivalAirportCode, serializer.SerializeString(fields[segmentsArrivalAirportCodePos]))
	dynMap.AddColumn(utils.SegmentsDepartureAirportCode, serializer.SerializeString(fields[segmentsDepartureAirportCodePos]))
	dynMap.AddColumn(utils.SegmentsAirlineName, serializer.SerializeString(fields[segmentsAirlineNamePos]))
	dynMap.AddColumn(utils.SegmentsAirlineCode, serializer.SerializeString(fields[segmentsAirlineCodePos]))
	dynMap.AddColumn(utils.SegmentsEquipmentDescription, serializer.SerializeString(fields[segmentsEquipmentDescriptionPos]))
	dynMap.AddColumn(utils.SegmentsDurationInSeconds, serializer.SerializeString(fields[segmentsDurationInSecondsPos]))
	dynMap.AddColumn(utils.SegmentsDistance, serializer.SerializeString(fields[segmentsDistancePos]))
	dynMap.AddColumn(utils.SegmentsCabinCode, serializer.SerializeString(fields[segmentsCabinCodePos]))

	return dynMap, nil
}

func (a FlightsParser) getTotalTravelDistance(fields []string) float32 {
	if fields[totalTravelDistancePos] != "" {
		totalTravelDistance, err := strconv.ParseFloat(fields[totalTravelDistancePos], 32)
		if err != nil {
			log.Warnf("FlightsParser | Error converting totalTravelDistance | %v | Will be sent as zero", err)
		}
		return float32(totalTravelDistance)
	}
	return 0
}

func (a FlightsParser) GetEofMsgType() int {
	return dataStructures.EOFFlightRows
}

func (a FlightsParser) GetMsgType() int {
	return dataStructures.FlightRows
}
