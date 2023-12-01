package controllers

import (
	"distance_completer/config"
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	queueProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	"github.com/brunograssano/Distribuidos-TP1/common/queuefactory"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

type DistanceCompleter struct {
	completerId  int
	airportsMaps map[string]map[string][2]float32
	c            *config.CompleterConfig
	consumer     queueProtocol.ConsumerProtocolInterface
	producer     queueProtocol.ProducerProtocolInterface
	prodForCons  queueProtocol.ProducerProtocolInterface
	checkpointer *checkpointer.CheckpointerHandler
}

func NewDistanceCompleter(
	id int,
	qFactory queuefactory.QueueProtocolFactory,
	c *config.CompleterConfig,
	chkHandler *checkpointer.CheckpointerHandler,
) *DistanceCompleter {
	consumer := qFactory.CreateConsumer(c.InputQueueFlightsName)
	producer := qFactory.CreateProducer(c.OutputQueueName)
	producerForCons := qFactory.CreateProducer(c.InputQueueFlightsName)
	chkHandler.AddCheckpointable(consumer, id)
	chkHandler.AddCheckpointable(producer, id)
	return &DistanceCompleter{
		completerId:  id,
		airportsMaps: make(map[string]map[string][2]float32),
		c:            c,
		consumer:     consumer,
		producer:     producer,
		prodForCons:  producerForCons,
		checkpointer: chkHandler,
	}
}

func (dc *DistanceCompleter) calculateDirectDistance(flightRow *dataStructures.DynamicMap, clientId string) (float32, error) {

	originId, errOri := flightRow.GetAsString(utils.StartingAirport)
	if errOri != nil {
		log.Errorf("DistanceCompleter %v | Error when trying to get originId | %v", dc.completerId, errOri)
	}
	origenAirport, exists := dc.airportsMaps[clientId][originId]
	if !exists {
		return -1, fmt.Errorf("row does not have correctly the origin airport. Skipping")
	}
	destinationId, errDest := flightRow.GetAsString(utils.DestinationAirport)
	if errDest != nil {
		log.Errorf("DistanceCompleter %v | Error when trying to get destinationId | %v", dc.completerId, errDest)
	}
	destinationAirport, exists := dc.airportsMaps[clientId][destinationId]
	if !exists {
		return -1, fmt.Errorf("row does not have correctly the destination airport. Skipping")
	}

	return float32(CalculateDistanceFrom(origenAirport, destinationAirport)), nil
}

func (dc *DistanceCompleter) addColumnToRow(key string, value float32, row *dataStructures.DynamicMap) {
	bytes := serializer.SerializeFloat(value)
	row.AddColumn(key, bytes)
}

func (dc *DistanceCompleter) calculateTotalTravelDistance(flightRow *dataStructures.DynamicMap, clientId string) (float32, error) {
	route, err := flightRow.GetAsString(utils.Route)
	if err != nil {
		log.Errorf("DistanceCompleter %v | Could not get the Route | %v", dc.completerId, err)
	}
	idsArray := strings.Split(route, utils.DoublePipeSeparator)
	totalTravelDistance := 0.0
	for i := 0; i < len(idsArray)-1; i++ {
		initialAirport, exists := dc.airportsMaps[clientId][idsArray[i]]
		if !exists {
			log.Errorf("DistanceCompleter %v | Row does not have correctly the route '%v' in '%v' | Skipping...", dc.completerId, idsArray[i], route)
			return -1, fmt.Errorf("row does not have correctly the route. Skipping")
		}
		nextAirport, exists := dc.airportsMaps[clientId][idsArray[i+1]]
		if !exists {
			log.Errorf("DistanceCompleter %v | Row does not have correctly the route '%v' in '%v'  | Skipping...", dc.completerId, idsArray[i+1], route)
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
	}
}

func (dc *DistanceCompleter) loadAirports(clientId string) {
	reader, err := filemanager.NewFileReader(fmt.Sprintf("%v_%v%v", dc.c.AirportsFilename, clientId, utils.CsvSuffix))
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
	dc.airportsMaps[clientId] = make(map[string][2]float32)
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
		dc.airportsMaps[clientId][id] = [2]float32{float32(lat), float32(long)}
	}
	err = reader.FileManager.Close()
	if err != nil {
		log.Errorf("DistanceCompleter %v | Error trying to close file: %v | %v", dc.completerId, dc.c.AirportsFilename+"_"+clientId+utils.CsvSuffix, err)
	}
}

func (dc *DistanceCompleter) CompleteDistances() {
	for {
		msg, ok := dc.consumer.Pop()
		if !ok {
			log.Infof("DistanceCompleter %v | Closing goroutine...", dc.completerId)
			return
		}
		log.Debugf("DistanceCompleter %v | Received Message | {type: %v, rowCount:%v}", dc.completerId, msg.TypeMessage, len(msg.DynMaps))
		if msg.TypeMessage == dataStructures.EOFFlightRows {
			log.Infof("DistanceCompleter %v | Received EOF. Handling...", dc.completerId)
			err := queueProtocol.HandleEOF(msg, dc.consumer, dc.prodForCons, []queueProtocol.ProducerProtocolInterface{dc.producer})
			if err != nil {
				log.Errorf("DistanceCompleter %v | Error handling EOF | %v", dc.completerId, err)
			}
			dc.clearInternalState(msg.ClientId)
		} else if msg.TypeMessage == dataStructures.FlightRows {
			log.Debugf("DistanceCompleter %v | Received Batch. Handling rows to be completed...", dc.completerId)
			dc.handleFlightRows(msg)
		} else {
			log.Warnf("DistanceCompleter %v | Warning Message | Unknown type of message: %v. Skipping it...", dc.completerId, msg.TypeMessage)
		}
		err := dc.checkpointer.DoCheckpoint(dc.completerId)
		if err != nil {
			log.Errorf("DistanceCompleter #%v | Error on checkpointing | %v", dc.completerId, err)
		}
	}
}

func (dc *DistanceCompleter) clearInternalState(clientId string) {
	delete(dc.airportsMaps, clientId)
}

func (dc *DistanceCompleter) checkForAirports(clientId string) bool {
	_, exists := dc.airportsMaps[clientId]
	if exists {
		return exists
	}
	exists = filemanager.DirectoryExists(dc.c.AirportsFilename + "_" + clientId + utils.CsvSuffix)
	if exists {
		dc.loadAirports(clientId)
	}
	return exists
}

func (dc *DistanceCompleter) handleFlightRows(msg *dataStructures.Message) {
	rows := msg.DynMaps
	existAirports := dc.checkForAirports(msg.ClientId)
	if !existAirports {
		dc.consumer.SetStatusOfLastMessage(false)
		return
	}
	var nextBatch []*dataStructures.DynamicMap
	for _, row := range rows {
		totalTravelDistance, err := row.GetAsFloat(utils.TotalTravelDistance)
		if err != nil || shouldCompleteCol(totalTravelDistance) {
			log.Debugf("DistanceCompleter %v | Row needs totalTravelDistance to be completed. Calculating...", dc.completerId)
			totalTravelDistance, err = dc.calculateTotalTravelDistance(row, msg.ClientId)
			if err != nil {
				log.Errorf("DistanceCompleter %v | Error adding totalTravelDistance | %v | Skipping row...", dc.completerId, err)
				continue
			}
			dc.addColumnToRow(utils.TotalTravelDistance, totalTravelDistance, row)
			log.Debugf("DistanceCompleter %v | totalTravelDistance added correctly!", dc.completerId)
		}
		log.Debugf("DistanceCompleter %v | Row needs directDistance to be completed. Calculating...", dc.completerId)
		directDistance, err := dc.calculateDirectDistance(row, msg.ClientId)
		if err != nil {
			log.Errorf("DistanceCompleter %v | Error adding directDistance | %v | Skipping row...", dc.completerId, err)
			continue
		}
		dc.addColumnToRow(utils.DirectDistance, directDistance, row)
		log.Debugf("DistanceCompleter %v | directDistance added correctly!", dc.completerId)
		nextBatch = append(nextBatch, row)
	}
	log.Debugf("DistanceCompleter %v | Finished processing batch. Sending to next queue...", dc.completerId)
	msgToSend := dataStructures.NewMessageWithData(msg, nextBatch)
	dc.sendNext(msgToSend)
}
