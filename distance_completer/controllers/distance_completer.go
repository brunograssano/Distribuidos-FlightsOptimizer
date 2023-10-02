package controllers

import (
	"distance_completer/config"
	"encoding/binary"
	"fmt"
	"github.com/asmarques/geodist"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	log "github.com/sirupsen/logrus"
	"math"
	"strconv"
	"strings"
)

type DistanceCompleter struct {
	completerId      int
	airportsMap      map[string][2]float32
	c                *config.CompleterConfig
	consumer         middleware.ConsumerInterface
	producer         middleware.ProducerInterface
	serializer       *dataStructures.DynamicMapSerializer
	fileLoadedSignal chan bool
}

const KmToMiles = 1 / 1.60934

func NewDistanceCompleter(
	id int,
	qMiddleware *middleware.QueueMiddleware,
	c *config.CompleterConfig,
	s *dataStructures.DynamicMapSerializer,
	fileLoadedSignal chan bool,
) *DistanceCompleter {
	consumer := qMiddleware.CreateConsumer(c.InputQueueFlightsName, true)
	producer := qMiddleware.CreateProducer(c.OutputQueueName, true)

	return &DistanceCompleter{
		completerId:      id,
		airportsMap:      make(map[string][2]float32),
		c:                c,
		consumer:         consumer,
		producer:         producer,
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

	originPoint := geodist.Point{Lat: float64(origenAirport[0]), Long: float64(origenAirport[1])}
	destPoint := geodist.Point{Lat: float64(destinationAirport[0]), Long: float64(destinationAirport[1])}
	directDistance := geodist.HaversineDistance(originPoint, destPoint)

	return float32(directDistance) * KmToMiles, nil
}

func (dc *DistanceCompleter) addColumnToRow(key string, value float32, row *dataStructures.DynamicMap) {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, math.Float32bits(value))
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

		originPoint := geodist.Point{Lat: float64(initialAirport[0]), Long: float64(initialAirport[1])}
		destPoint := geodist.Point{Lat: float64(nextAirport[0]), Long: float64(nextAirport[1])}
		segmentDistance := geodist.HaversineDistance(originPoint, destPoint)

		totalTravelDistance += segmentDistance * KmToMiles
	}
	return float32(totalTravelDistance), nil
}

func shouldCompleteCol(distance float32) bool {
	return distance == 0
}

func (dc *DistanceCompleter) sendNext(row *dataStructures.DynamicMap) {
	bytesToSend := dc.serializer.Serialize(row)
	err := dc.producer.Send(bytesToSend)
	if err != nil {
		log.Errorf("Error trying to send to the next service...")
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
		row := dc.serializer.Deserialize(msg)
		totalTravelDistance, err := row.GetAsFloat("totalTravelDistance")
		if err != nil || shouldCompleteCol(totalTravelDistance) {
			totalTravelDistance, err = dc.calculateTotalTravelDistance(row)
			if err != nil {
				continue
			}
			dc.addColumnToRow("totalTravelDistance", totalTravelDistance, row)
		}
		directDistance, err := dc.calculateDirectDistance(row)
		if err != nil {
			continue
		}
		dc.addColumnToRow("directDistance", directDistance, row)
		dc.sendNext(row)
	}
}
