package controllers

import (
	"distance_completer/config"
	"fmt"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
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
	serializer       *dataStructures.Serializer
	fileLoadedSignal chan bool
}

func NewDistanceCompleter(
	id int,
	qMiddleware *middleware.QueueMiddleware,
	c *config.CompleterConfig,
	s *dataStructures.Serializer,
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
		serializer:       s,
		fileLoadedSignal: fileLoadedSignal,
	}
}

func (dc *DistanceCompleter) calculateDirectDistance(flightRow *dataStructures.DynamicMap) (float32, error) {

	originId, errOri := flightRow.GetAsString("startingAirport")
	if errOri != nil {
		log.Errorf("Error when trying to get originId: %v", errOri)
	}
	origenAirport, exists := dc.airportsMap[originId]
	if !exists {
		return -1, fmt.Errorf("row does not have correctly the origin airport. Skipping")
	}
	destinationId, errDest := flightRow.GetAsString("destinationAirport")
	if errDest != nil {
		log.Errorf("Error when trying to get destinationId: %v", errDest)
	}
	destinationAirport, exists := dc.airportsMap[destinationId]
	if !exists {
		return -1, fmt.Errorf("row does not have correctly the destination airport. Skipping")
	}

	return float32(CalculateDistanceFrom(origenAirport, destinationAirport)), nil
}

func (dc *DistanceCompleter) addColumnToRow(key string, value float32, row *dataStructures.DynamicMap) {
	bytes := dc.serializer.SerializeFloat(value)
	row.AddColumn(key, bytes)
}

func (dc *DistanceCompleter) calculateTotalTravelDistance(flightRow *dataStructures.DynamicMap) (float32, error) {
	route, err := flightRow.GetAsString("route")
	if err != nil {
		log.Errorf("Could not get the Route: %v", err)
	}
	idsArray := strings.Split(route, "||")
	totalTravelDistance := 0.0
	for i := 0; i < len(idsArray)-1; i++ {
		initialAirport, exists := dc.airportsMap[idsArray[i]]
		if !exists {
			log.Errorf("Row does not have correctly the route. Skipping...")
			return -1, fmt.Errorf("row does not have correctly the route. Skipping")
		}
		nextAirport, exists := dc.airportsMap[idsArray[i+1]]
		if !exists {
			log.Errorf("Row does not have correctly the route. Skipping...")
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
		log.Errorf("Error trying to send to the next service...")
	} else {
		log.Infof("Message sent correctly to the consumer...")
	}
}

func (dc *DistanceCompleter) loadAirports() {
	reader, err := filemanager.NewFileReader(dc.c.AirportsFilename)
	if err != nil {
		log.Errorf("Error trying to read the airports: %v", err)
		if reader != nil {
			err = reader.FileManager.Close()
			if err != nil {
				log.Errorf("Error trying to close FileManager: %v", err)
			}
		}
		return
	}
	for reader.CanRead() {
		csvAirport := reader.ReadLine()
		idLatLong := strings.Split(csvAirport, ",")
		id := idLatLong[0]
		lat, err := strconv.ParseFloat(idLatLong[1], 32)
		if err != nil {
			log.Fatalf("Error trying to cast latitude: %v", err)
		}
		long, err := strconv.ParseFloat(idLatLong[2], 32)
		if err != nil {
			log.Fatalf("Error trying to cast latitude: %v", err)
		}
		dc.airportsMap[id] = [2]float32{float32(lat), float32(long)}
	}
	err = reader.FileManager.Close()
	if err != nil {
		log.Errorf("Error trying to close file: %v. Error was: %v", dc.c.AirportsFilename, err)
	}
}

func (dc *DistanceCompleter) CompleteDistances() {
	<-dc.fileLoadedSignal
	log.Infof("[CompleterProcess] Received signal to load file. Loading airports and initializing completer...")
	dc.loadAirports()
	for {
		msg, ok := dc.consumer.Pop()
		if !ok {
			log.Infof("Closing goroutine %v", dc.completerId)
			return
		}
		log.Debugf("Received Message: {type: %v, rowCount:%v}", msg.TypeMessage, len(msg.DynMaps))
		if msg.TypeMessage == dataStructures.EOFFlightRows {
			log.Infof("[CompleterProcess] Received EOF. Handling...")
			err := protocol.HandleEOF(msg, dc.consumer, dc.prodForCons, []protocol.ProducerProtocolInterface{dc.producer})
			if err != nil {
				log.Errorf("Error handling EOF: %v", err)
			}
			return
		} else if msg.TypeMessage == dataStructures.FlightRows {
			log.Infof("[CompleterProcess] Received Batch. Handling rows to be completed...")
			dc.handleFlightRows(msg)
		} else {
			log.Warnf("Unknown type of message: %v. Skipping it...", msg.TypeMessage)
		}
	}
}

func (dc *DistanceCompleter) handleFlightRows(msg *dataStructures.Message) {
	rows := msg.DynMaps
	var nextBatch []*dataStructures.DynamicMap
	for _, row := range rows {
		totalTravelDistance, err := row.GetAsFloat("totalTravelDistance")
		if err != nil || shouldCompleteCol(totalTravelDistance) {
			log.Debugf("Row needs totalTravelDistance to be completed. Calculating...")
			totalTravelDistance, err = dc.calculateTotalTravelDistance(row)
			if err != nil {
				log.Errorf("Error adding totalTravelDistance: %v. Skipping row...", err)
				continue
			}
			dc.addColumnToRow("totalTravelDistance", totalTravelDistance, row)
			log.Debugf("totalTravelDistance added correctly!")
		}
		log.Debugf("Row needs directDistance to be completed. Calculating...")
		directDistance, err := dc.calculateDirectDistance(row)
		if err != nil {
			log.Errorf("Error adding directDistance: %v. Skipping row...", err)
			continue
		}
		dc.addColumnToRow("directDistance", directDistance, row)
		log.Debugf("directDistance added correctly!")
		nextBatch = append(nextBatch, row)
	}
	log.Infof("[CompleterProcess] Finished processing batch. Sending to next queue...")
	msgToSend := &dataStructures.Message{TypeMessage: dataStructures.FlightRows, DynMaps: nextBatch}
	dc.sendNext(msgToSend)
}
