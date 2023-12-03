package main

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
	dataStructure "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	queueProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"slices"
	"strconv"
	"strings"
)

// JourneySaver Handles the prices of the assigned journeys
type JourneySaver struct {
	consumer               queueProtocol.ConsumerProtocolInterface
	accumProducer          queueProtocol.ProducerProtocolInterface
	avgAndMaxProducer      queueProtocol.ProducerProtocolInterface
	partialResultsByClient map[string]*PartialResult
	processedClients       map[string]bool
	totalSaversCount       uint
	checkpointer           *checkpointer.CheckpointerHandler
	id                     int
}

// NewJourneySaver Creates a new JourneySaver
func NewJourneySaver(
	consumer queueProtocol.ConsumerProtocolInterface,
	accumProducer queueProtocol.ProducerProtocolInterface,
	avgAndMaxProducer queueProtocol.ProducerProtocolInterface,
	totalSaversCount uint,
	chkHandler *checkpointer.CheckpointerHandler,
	id uint,
) *JourneySaver {
	js := &JourneySaver{
		consumer:               consumer,
		accumProducer:          accumProducer,
		avgAndMaxProducer:      avgAndMaxProducer,
		partialResultsByClient: make(map[string]*PartialResult),
		processedClients:       make(map[string]bool),
		totalSaversCount:       totalSaversCount,
		checkpointer:           chkHandler,
		id:                     int(id),
	}
	chkHandler.AddCheckpointable(consumer, js.id)
	chkHandler.AddCheckpointable(js, js.id)
	return js
}

func (js *JourneySaver) saveRowsInFiles(dynMaps []*dataStructure.DynamicMap, clientId string) {
	// May need optimization. Mix of memory and disk or only memory...
	log.Debugf("JourneySaver %v | Writing records to files", js.id)
	for _, dynMap := range dynMaps {
		stAirport, err := dynMap.GetAsString(utils.StartingAirport)
		if err != nil {
			log.Errorf("JourneySaver %v | Error getting starting airport | %v | Skipping row...", js.id, err)
			continue
		}
		destAirport, err := dynMap.GetAsString(utils.DestinationAirport)
		if err != nil {
			log.Errorf("JourneySaver %v | Error getting destination airport | %v | Skipping row...", js.id, err)
			continue
		}
		totalFare, err := dynMap.GetAsFloat(utils.TotalFare)
		if err != nil {
			log.Errorf("JourneySaver %v | Error getting total fare | %v | Skipping row...", js.id, err)
			continue
		}
		if totalFare <= 0 {
			log.Errorf("JourneySaver %v | Total fare <= 0 | Skipping row...", js.id)
			continue
		}
		js.writeResults(stAirport, destAirport, clientId, err, totalFare)
	}
}

func (js *JourneySaver) writeResults(stAirport string, destAirport string, clientId string, err error, totalFare float32) {
	journey := fmt.Sprintf("%v-%v_%v", stAirport, destAirport, clientId)
	fileWriter, err := filemanager.NewFileWriter(journey)
	if err != nil {
		log.Errorf("JourneySaver %v | Error creating file writer for %v | %v | Skipping row...", js.id, journey, err)
		return
	}
	err = fileWriter.WriteLine(fmt.Sprintf("%v\n", totalFare))
	if err != nil {
		log.Errorf("JourneySaver %v | Error writing total fare with file writer | %v | Skipping row...", js.id, err)
		return
	}
	log.Debugf("JourneySaver %v | Added price %v to registry of journey: %v", js.id, totalFare, journey)
	partialResult := js.getPartialResultOfClient(clientId)
	if !slices.Contains(partialResult.filesToRead, journey) {
		log.Infof("JourneySaver %v | Adding journey: %v to the files that must be read.", js.id, journey)
		partialResult.filesToRead = append(partialResult.filesToRead, journey)
	}
	err = fileWriter.FileManager.Close()
	if err != nil {
		log.Errorf("JourneySaver %v | Error closing file manager for journey %v | %v", js.id, journey, err)
	}
	partialResult.totalPrice += totalFare
	partialResult.quantities++
}

func (js *JourneySaver) getPartialResultOfClient(clientId string) *PartialResult {
	partialResult, exists := js.partialResultsByClient[clientId]
	if exists {
		return partialResult
	}
	js.partialResultsByClient[clientId] = NewPartialResult()
	return js.partialResultsByClient[clientId]
}

func (js *JourneySaver) readJourneyAsArrays(journeyStr string) ([]float32, error) {
	fileReader, err := filemanager.NewFileReader(journeyStr)
	var prices []float32
	for fileReader.CanRead() {
		individualPrice, err := strconv.ParseFloat(fileReader.ReadLine(), 32)
		if err != nil {
			log.Errorf("JourneySaver %v | Error reading price | %v | Skipping row...", js.id, err)
			continue
		}
		log.Debugf("JourneySaver %v | Read price: %v", js.id, individualPrice)
		prices = append(prices, float32(individualPrice))
	}
	err = fileReader.FileManager.Close()
	if err != nil {
		log.Errorf("JourneySaver %v | Error closing fileReader: %v | %v", js.id, journeyStr, err)
		return prices, err
	}
	return prices, nil
}

// sendToGeneralAccumulator Sends to the accumulator the values that the JourneySaver managed
func (js *JourneySaver) sendToGeneralAccumulator(oldMsg *dataStructure.Message) error {
	dynMapData := make(map[string][]byte)
	partialResult, exists := js.partialResultsByClient[oldMsg.ClientId]
	if !exists {
		partialResult = NewPartialResult()
	}
	dynMapData[utils.LocalPrice] = serializer.SerializeFloat(partialResult.totalPrice)
	dynMapData[utils.LocalQuantity] = serializer.SerializeUint(uint32(partialResult.quantities))
	msgToSend := dataStructure.NewTypeMessageWithDataAndMsgId(dataStructure.EOFFlightRows, oldMsg, []*dataStructure.DynamicMap{dataStructure.NewDynamicMap(dynMapData)}, oldMsg.MessageId+uint(oldMsg.RowId))
	log.Infof("JourneySaver %v | Received EOF. Sending to Gral Accum. TotalPrice: %v, Quantities: %v | ID: %v-%v-%v", js.id, partialResult.totalPrice, partialResult.quantities, msgToSend.ClientId, msgToSend.MessageId, msgToSend.RowId)
	err := js.accumProducer.Send(msgToSend)
	if err != nil {
		log.Errorf("JourneySaver %v | Error trying to send to general accumulator | %v", js.id, err)
		return err
	}
	return nil
}

// filterGreaterThanAverage Returns the prices greater than the average
func (js *JourneySaver) filterGreaterThanAverage(prices []float32, avg float32) []float32 {
	var returnArray []float32
	for _, price := range prices {
		if price > avg {
			returnArray = append(returnArray, price)
		}
	}
	return returnArray
}

func (js *JourneySaver) getMaxAndAverage(prices []float32) (float32, float32) {
	maxVal := float32(0)
	accumPrices := float32(0)
	accumCount := float32(0)
	for _, price := range prices {
		if price > maxVal {
			maxVal = price
		}
		accumPrices += price
		accumCount++
	}
	average := float32(0)
	if accumCount > 0 {
		average = accumPrices / accumCount
	}
	return average, maxVal
}

func (js *JourneySaver) sendAverageForJourneys(finalAvg float32, msg *dataStructure.Message) {
	partialResults, exist := js.partialResultsByClient[msg.ClientId]
	if !exist {
		partialResults = NewPartialResult()
	}
	var data []*dataStructure.DynamicMap
	for _, fileStr := range partialResults.filesToRead {
		log.Debugf("JourneySaver %v | Reading file: %v", js.id, fileStr)
		pricesForJourney, err := js.readJourneyAsArrays(fileStr)
		if err != nil {
			log.Errorf("JourneySaver %v | Error reading file | %v | Skipping file...", js.id, err)
			continue
		}
		filteredPrices := js.filterGreaterThanAverage(pricesForJourney, finalAvg)
		log.Debugf("JourneySaver %v | Filtered prices. Original len: %v ; Filtered len: %v", js.id, len(pricesForJourney), len(filteredPrices))
		if len(filteredPrices) == 0 {
			log.Warnf("JourneySaver %v | Filtered Prices Length is 0 | Skipping...", js.id)
			continue
		}
		journeyAverage, journeyMax := js.getMaxAndAverage(filteredPrices)
		log.Debugf("JourneySaver | Average: %v, Maximum: %v", journeyAverage, journeyMax)

		dynMap := make(map[string][]byte)
		dynMap[utils.Avg] = serializer.SerializeFloat(journeyAverage)
		dynMap[utils.Max] = serializer.SerializeFloat(journeyMax)
		dynMap[utils.Journey] = serializer.SerializeString(strings.Split(fileStr, "_")[0])
		data = append(data, dataStructure.NewDynamicMap(dynMap))
	}
	log.Debugf("JourneySaver %v | Sending max and avg to next step...", js.id)
	err := js.avgAndMaxProducer.Send(dataStructure.NewTypeMessageWithData(dataStructure.FlightRows, msg, data))
	if err != nil {
		log.Errorf("JourneySaver %v | Error sending to saver the journeys | %v | Skipping...", js.id, err)
	}
	err = js.avgAndMaxProducer.Send(dataStructure.NewTypeMessageWithoutDataAndMsgId(dataStructure.EOFFlightRows, msg, msg.MessageId+js.totalSaversCount))
	if err != nil {
		log.Errorf("JourneySaver %v | Error sending EOF to saver | %v", js.id, err)
	}
	err = filemanager.MoveFiles(partialResults.filesToRead, msg.ClientId)
	if err != nil {
		log.Errorf("JourneySaver %v | Error trying to move files | %v", js.id, err)
	}
	js.clearInternalState(msg.ClientId)
}

// SavePricesForJourneys JourneySaver loop that reads from the input channel, saves the journey and performs calculations
func (js *JourneySaver) SavePricesForJourneys() {
	for {
		msg, ok := js.consumer.Pop()
		if !ok {
			log.Errorf("JourneySaver %v | Input of messages closed. Ending execution...", js.id)
			return
		}
		log.Debugf("JourneySaver %v | Received message of type: %v. Row Count: %v", js.id, msg.TypeMessage, len(msg.DynMaps))
		if msg.TypeMessage == dataStructure.EOFFlightRows {
			err := js.sendToGeneralAccumulator(msg)
			if err != nil {
				log.Errorf("JourneySaver %v | Could not send to General Accumulator. Ending execution...", js.id)
				return
			}
			log.Debugf("JourneySaver %v | Sent correctly!", js.id)
		} else if msg.TypeMessage == dataStructure.FlightRows {
			log.Debugf("JourneySaver %v | Received flight row. Now saving...", js.id)
			js.saveRowsInFiles(msg.DynMaps, msg.ClientId)
		} else if msg.TypeMessage == dataStructure.FinalAvgMsg {
			finalAvg, err := msg.DynMaps[0].GetAsFloat(utils.FinalAvg)
			if err != nil {
				log.Errorf("JourneySaver %v | Error getting finalAvg", js.id)
			}
			log.Infof("JourneySaver %v | Received Final Avg: %v | Client %v | Checking if it was already processed...", js.id, finalAvg, msg.ClientId)
			_, exists := js.processedClients[msg.ClientId]
			if !exists {
				log.Infof("JourneySaver %v | It was not processed | Client %v | Now sending Average for Journeys...", js.id, msg.ClientId)
				js.sendAverageForJourneys(finalAvg, msg)
			} else {
				log.Infof("JourneySaver %v | Message was duplicated | Client %v | Discarding it... ", js.id, msg.ClientId)
			}

		}
		err := js.checkpointer.DoCheckpoint(js.id)
		if err != nil {
			log.Errorf("JourneySaver %v | Error doing checkpointing | %v", js.id, err)
		}
	}
}

func (js *JourneySaver) clearInternalState(clientId string) {
	delete(js.partialResultsByClient, clientId)
	js.processedClients[clientId] = true
}
