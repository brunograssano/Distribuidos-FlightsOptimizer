package main

import (
	"fmt"
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
}

// NewJourneySaver Creates a new JourneySaver
func NewJourneySaver(consumer queueProtocol.ConsumerProtocolInterface, accumProducer queueProtocol.ProducerProtocolInterface, avgAndMaxProducer queueProtocol.ProducerProtocolInterface) *JourneySaver {
	return &JourneySaver{consumer: consumer, accumProducer: accumProducer, avgAndMaxProducer: avgAndMaxProducer, partialResultsByClient: make(map[string]*PartialResult), processedClients: make(map[string]bool)}
}

func (js *JourneySaver) saveRowsInFiles(dynMaps []*dataStructure.DynamicMap, clientId string) {
	// May need optimization. Mix of memory and disk or only memory...
	log.Debugf("JourneySaver | Writing records to files")
	for _, dynMap := range dynMaps {
		stAirport, err := dynMap.GetAsString(utils.StartingAirport)
		if err != nil {
			log.Errorf("JourneySaver | Error getting starting airport | %v | Skipping row...", err)
			continue
		}
		destAirport, err := dynMap.GetAsString(utils.DestinationAirport)
		if err != nil {
			log.Errorf("JourneySaver | Error getting destination airport | %v | Skipping row...", err)
			continue
		}
		totalFare, err := dynMap.GetAsFloat(utils.TotalFare)
		if err != nil {
			log.Errorf("JourneySaver | Error getting total fare | %v | Skipping row...", err)
			continue
		}
		if totalFare <= 0 {
			log.Errorf("JourneySaver | Total fare <= 0 | Skipping row...")
			continue
		}
		js.writeResults(stAirport, destAirport, clientId, err, totalFare)
	}
}

func (js *JourneySaver) writeResults(stAirport string, destAirport string, clientId string, err error, totalFare float32) {
	journey := fmt.Sprintf("%v-%v_%v", stAirport, destAirport, clientId)
	fileWriter, err := filemanager.NewFileWriter(journey)
	if err != nil {
		log.Errorf("JourneySaver | Error creating file writer for %v | %v | Skipping row...", journey, err)
		return
	}
	err = fileWriter.WriteLine(fmt.Sprintf("%v\n", totalFare))
	if err != nil {
		log.Errorf("JourneySaver | Error writing total fare with file writer | %v | Skipping row...", err)
		return
	}
	log.Debugf("JourneySaver | Added price %v to registry of journey: %v", totalFare, journey)
	partialResult := js.getPartialResultOfClient(clientId)
	if !slices.Contains(partialResult.filesToRead, journey) {
		log.Infof("JourneySaver | Adding journey: %v to the files that must be read.", journey)
		partialResult.filesToRead = append(partialResult.filesToRead, journey)
	}
	err = fileWriter.FileManager.Close()
	if err != nil {
		log.Errorf("JourneySaver | Error closing file manager for journey %v | %v", journey, err)
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
			log.Errorf("JourneySaver | Error reading price | %v | Skipping row...", err)
			continue
		}
		log.Debugf("JourneySaver | Read price: %v", individualPrice)
		prices = append(prices, float32(individualPrice))
	}
	err = fileReader.FileManager.Close()
	if err != nil {
		log.Errorf("JourneySaver | Error closing fileReader: %v | %v", journeyStr, err)
		return prices, err
	}
	return prices, nil
}

// sendToGeneralAccumulator Sends to the accumulator the values that the JourneySaver managed
func (js *JourneySaver) sendToGeneralAccumulator(clientId string) error {
	dynMapData := make(map[string][]byte)
	partialResult, exists := js.partialResultsByClient[clientId]
	if !exists {
		partialResult = NewPartialResult()
	}
	log.Infof("JourneySaver | Received EOF. Sending to Gral Accum. TotalPrice: %v, Quantities: %v", partialResult.totalPrice, partialResult.quantities)
	dynMapData[utils.LocalPrice] = serializer.SerializeFloat(partialResult.totalPrice)
	dynMapData[utils.LocalQuantity] = serializer.SerializeUint(uint32(partialResult.quantities))
	msgToSend := &dataStructure.Message{
		TypeMessage: dataStructure.EOFFlightRows,
		DynMaps:     []*dataStructure.DynamicMap{dataStructure.NewDynamicMap(dynMapData)},
		ClientId:    clientId,
	}
	err := js.accumProducer.Send(msgToSend)
	if err != nil {
		log.Errorf("JourneySaver | Error trying to send to general accumulator | %v", err)
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

func (js *JourneySaver) sendAverageForJourneys(finalAvg float32, clientId string) {
	partialResults, exist := js.partialResultsByClient[clientId]
	if !exist {
		partialResults = NewPartialResult()
	}
	for _, fileStr := range partialResults.filesToRead {
		log.Debugf("JourneySaver | Reading file: %v", fileStr)
		pricesForJourney, err := js.readJourneyAsArrays(fileStr)
		if err != nil {
			log.Errorf("JourneySaver | Error reading file | %v | Skipping file...", err)
			continue
		}
		filteredPrices := js.filterGreaterThanAverage(pricesForJourney, finalAvg)
		log.Debugf("JourneySaver | Filtered prices. Original len: %v ; Filtered len: %v", len(pricesForJourney), len(filteredPrices))
		if len(filteredPrices) == 0 {
			log.Warnf("JourneySaver | Filtered Prices Length is 0 | Skipping...")
			continue
		}
		journeyAverage, journeyMax := js.getMaxAndAverage(filteredPrices)
		log.Debugf("JourneySaver | Average: %v, Maximum: %v", journeyAverage, journeyMax)

		dynMap := make(map[string][]byte)
		dynMap[utils.Avg] = serializer.SerializeFloat(journeyAverage)
		dynMap[utils.Max] = serializer.SerializeFloat(journeyMax)
		dynMap[utils.Journey] = serializer.SerializeString(strings.Split(fileStr, "_")[0])
		data := []*dataStructure.DynamicMap{dataStructure.NewDynamicMap(dynMap)}
		msg := &dataStructure.Message{
			TypeMessage: dataStructure.FlightRows,
			DynMaps:     data,
			ClientId:    clientId,
		}
		log.Debugf("JourneySaver | Sending max and avg to next step...")
		err = js.avgAndMaxProducer.Send(msg)
		if err != nil {
			log.Errorf("JourneySaver | Error sending to saver the journey %v | %v | Skipping...", fileStr, err)
			continue
		}
	}
	err := js.avgAndMaxProducer.Send(&dataStructure.Message{TypeMessage: dataStructure.EOFFlightRows, ClientId: clientId})
	if err != nil {
		log.Errorf("JourneySaver | Error sending EOF to saver | %v", err)
	}
	err = filemanager.MoveFiles(partialResults.filesToRead, clientId)
	if err != nil {
		log.Errorf("JourneySaver | Error trying to move files | %v", err)
	}
	js.clearInternalState(clientId)
}

// SavePricesForJourneys JourneySaver loop that reads from the input channel, saves the journey and performs calculations
func (js *JourneySaver) SavePricesForJourneys() {
	for {
		msg, ok := js.consumer.Pop()
		if !ok {
			log.Errorf("JourneySaver | Input of messages closed. Ending execution...")
			return
		}
		log.Debugf("JourneySaver | Received message of type: %v. Row Count: %v", msg.TypeMessage, len(msg.DynMaps))
		if msg.TypeMessage == dataStructure.EOFFlightRows {
			err := js.sendToGeneralAccumulator(msg.ClientId)
			if err != nil {
				log.Errorf("JourneySaver | Could not send to General Accumulator. Ending execution...")
				return
			}
			log.Debugf("JourneySaver | Sent correctly!")
		} else if msg.TypeMessage == dataStructure.FlightRows {
			log.Debugf("JourneySaver | Received flight row. Now saving...")
			js.saveRowsInFiles(msg.DynMaps, msg.ClientId)
		} else if msg.TypeMessage == dataStructure.FinalAvgMsg {
			finalAvg, err := msg.DynMaps[0].GetAsFloat(utils.FinalAvg)
			if err != nil {
				log.Errorf("JourneySaver | Error getting finalAvg")
			}
			log.Infof("JourneySaver | Received Final Avg: %v | Client %v | Checking if it was already processed...", finalAvg, msg.ClientId)
			_, exists := js.processedClients[msg.ClientId]
			if !exists {
				log.Infof("JourneySaver | It was not processed | Client %v | Now sending Average for Journeys...", msg.ClientId)
				js.sendAverageForJourneys(finalAvg, msg.ClientId)
			} else {
				log.Infof("JourneySaver | Message was duplicated | Client %v | Discarding it... ", msg.ClientId)
			}

		}
	}
}

func (js *JourneySaver) clearInternalState(clientId string) {
	delete(js.partialResultsByClient, clientId)
	js.processedClients[clientId] = true
}
