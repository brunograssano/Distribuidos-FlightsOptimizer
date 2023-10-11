package controllers

import (
	"distance_completer/config"
	"fmt"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

type DistanceCompleter struct {
	completerId      int
	airportsMap      map[string][2]float32
	c                *config.CompleterConfig
	consumer         protocol.ConsumerProtocolInterface
	producer         protocol.ProducerProtocolInterface
	prodForCons      protocol.ProducerProtocolInterface
	fileLoadedSignal chan bool
}

func NewDistanceCompleter(
	id int,
	qMiddleware *middleware.QueueMiddleware,
	c *config.CompleterConfig,
	fileLoadedSignal chan bool,
) *DistanceCompleter {
	consumer := protocol.NewConsumerQueueProtocolHandler(qMiddleware.CreateConsumer(c.InputQueueFlightsName, true))
	producer := protocol.NewProducerQueueProtocolHandler(qMiddleware.CreateProducer(c.OutputQueueName, true))
	producerForCons := protocol.NewProducerQueueProtocolHandler(qMiddleware.CreateProducer(c.InputQueueFlightsName, true))

	return &DistanceCompleter{
		completerId:      id,
		airportsMap:      make(map[string][2]float32),
		c:                c,
		consumer:         consumer,
		producer:         producer,
		prodForCons:      producerForCons,
		fileLoadedSignal: fileLoadedSignal,
	}
}

func (dc *DistanceCompleter) calculateDirectDistance(flightRow *dataStructures.DynamicMap) (float32, error) {

	originId, errOri := flightRow.GetAsString(utils.StartingAirport)
	if errOri != nil {
		log.Errorf("DistanceCompleter %v | Error when trying to get originId | %v", dc.completerId, errOri)
	}
	origenAirport, exists := dc.airportsMap[originId]
	if !exists {
		return -1, fmt.Errorf("row does not have correctly the origin airport. Skipping")
	}
	destinationId, errDest := flightRow.GetAsString(utils.DestinationAirport)
	if errDest != nil {
		log.Errorf("DistanceCompleter %v | Error when trying to get destinationId | %v", dc.completerId, errDest)
	}
	destinationAirport, exists := dc.airportsMap[destinationId]
	if !exists {
		return -1, fmt.Errorf("row does not have correctly the destination airport. Skipping")
	}

	return float32(CalculateDistanceFrom(origenAirport, destinationAirport)), nil
}

func (dc *DistanceCompleter) addColumnToRow(key string, value float32, row *dataStructures.DynamicMap) {
	bytes := serializer.SerializeFloat(value)
	row.AddColumn(key, bytes)
}

func (dc *DistanceCompleter) calculateTotalTravelDistance(flightRow *dataStructures.DynamicMap) (float32, error) {
	route, err := flightRow.GetAsString(utils.Route)
	if err != nil {
		log.Errorf("DistanceCompleter %v | Could not get the Route | %v", dc.completerId, err)
	}
	idsArray := strings.Split(route, utils.DoublePipeSeparator)
	totalTravelDistance := 0.0
	for i := 0; i < len(idsArray)-1; i++ {
		initialAirport, exists := dc.airportsMap[idsArray[i]]
		if !exists {
			log.Errorf("DistanceCompleter %v | Row does not have correctly the route | Skipping...", dc.completerId)
			return -1, fmt.Errorf("row does not have correctly the route. Skipping")
		}
		nextAirport, exists := dc.airportsMap[idsArray[i+1]]
		if !exists {
			log.Errorf("DistanceCompleter %v | Row does not have correctly the route | Skipping...", dc.completerId)
			return -1, fmt.Errorf("row does not have correctly the route. Skipping")
		}

		totalTravelDistance += CalculateDistanceFrom(initialAirport, nextAirport)
	}
	return float32(totalTravelDistance), nil
}

func shouldCompleteCol(distance float32) bool {
	return distance == 0
}

func (dc *DistanceCompleter) sendNext(message *dataStructures.Message) {
	err := dc.producer.Send(message)
	if err != nil {
		log.Errorf("DistanceCompleter %v | Error trying to send to the next service | %v", dc.completerId, err)
	} else {
		log.Infof("DistanceCompleter %v | Message sent correctly to the consumer...", dc.completerId)
	}
}

func (dc *DistanceCompleter) loadAirports() {
	reader, err := filemanager.NewFileReader(dc.c.AirportsFilename)
	if err != nil {
		log.Errorf("DistanceCompleter %v | Error trying to read the airports | %v", dc.completerId, err)
		if reader != nil {
			err = reader.FileManager.Close()
			if err != nil {
				log.Errorf("DistanceCompleter %v | Error trying to close FileManager | %v", dc.completerId, err)
			}
		}
		return
	}
	for reader.CanRead() {
		csvAirport := reader.ReadLine()
		idLatLong := strings.Split(csvAirport, utils.CommaSeparator)
		id := idLatLong[0]
		lat, err := strconv.ParseFloat(idLatLong[1], 32)
		if err != nil {
			log.Fatalf("DistanceCompleter %v | Error trying to cast latitude | %v", dc.completerId, err)
		}
		long, err := strconv.ParseFloat(idLatLong[2], 32)
		if err != nil {
			log.Fatalf("DistanceCompleter %v | Error trying to cast latitude | %v", dc.completerId, err)
		}
		dc.airportsMap[id] = [2]float32{float32(lat), float32(long)}
	}
	err = reader.FileManager.Close()
	if err != nil {
		log.Errorf("DistanceCompleter %v | Error trying to close file: %v | %v", dc.completerId, dc.c.AirportsFilename, err)
	}
}

func (dc *DistanceCompleter) CompleteDistances() {
	<-dc.fileLoadedSignal
	log.Infof("DistanceCompleter %v | Received signal to load file | Loading airports and initializing completer...", dc.completerId)
	dc.loadAirports()
	for {
		msg, ok := dc.consumer.Pop()
		if !ok {
			log.Infof("DistanceCompleter %v | Closing goroutine...", dc.completerId)
			return
		}
		log.Debugf("DistanceCompleter %v | Received Message | {type: %v, rowCount:%v}", dc.completerId, msg.TypeMessage, len(msg.DynMaps))
		if msg.TypeMessage == dataStructures.EOFFlightRows {
			log.Infof("DistanceCompleter %v | Received EOF. Handling...", dc.completerId)
			err := protocol.HandleEOF(msg, dc.consumer, dc.prodForCons, []protocol.ProducerProtocolInterface{dc.producer})
			if err != nil {
				log.Errorf("DistanceCompleter %v | Error handling EOF | %v", dc.completerId, err)
			}
			return
		} else if msg.TypeMessage == dataStructures.FlightRows {
			log.Infof("DistanceCompleter %v | Received Batch. Handling rows to be completed...", dc.completerId)
			dc.handleFlightRows(msg)
		} else {
			log.Warnf("DistanceCompleter %v | Warning Message | Unknown type of message: %v. Skipping it...", dc.completerId, msg.TypeMessage)
		}
	}
}

func (dc *DistanceCompleter) handleFlightRows(msg *dataStructures.Message) {
	rows := msg.DynMaps
	var nextBatch []*dataStructures.DynamicMap
	for _, row := range rows {
		totalTravelDistance, err := row.GetAsFloat("totalTravelDistance")
		if err != nil || shouldCompleteCol(totalTravelDistance) {
			log.Debugf("DistanceCompleter %v | Row needs totalTravelDistance to be completed. Calculating...", dc.completerId)
			totalTravelDistance, err = dc.calculateTotalTravelDistance(row)
			if err != nil {
				log.Errorf("DistanceCompleter %v | Error adding totalTravelDistance | %v | Skipping row...", dc.completerId, err)
				continue
			}
			dc.addColumnToRow("totalTravelDistance", totalTravelDistance, row)
			log.Debugf("DistanceCompleter %v | totalTravelDistance added correctly!", dc.completerId)
		}
		log.Debugf("DistanceCompleter %v | Row needs directDistance to be completed. Calculating...", dc.completerId)
		directDistance, err := dc.calculateDirectDistance(row)
		if err != nil {
			log.Errorf("DistanceCompleter %v | Error adding directDistance | %v | Skipping row...", dc.completerId, err)
			continue
		}
		dc.addColumnToRow("directDistance", directDistance, row)
		log.Debugf("DistanceCompleter %v | directDistance added correctly!", dc.completerId)
		nextBatch = append(nextBatch, row)
	}
	log.Infof("DistanceCompleter %v | Finished processing batch. Sending to next queue...", dc.completerId)
	msgToSend := &dataStructures.Message{TypeMessage: dataStructures.FlightRows, DynMaps: nextBatch}
	dc.sendNext(msgToSend)
}
