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
)

// JourneySaver Handles the prices of the assigned journeys
type JourneySaver struct {
	consumer          queueProtocol.ConsumerProtocolInterface
	accumProducer     queueProtocol.ProducerProtocolInterface
	avgAndMaxProducer queueProtocol.ProducerProtocolInterface
	filesToRead       []string
	totalPrice        float32
	quantities        int
}

// NewJourneySaver Creates a new JourneySaver
func NewJourneySaver(consumer queueProtocol.ConsumerProtocolInterface, accumProducer queueProtocol.ProducerProtocolInterface, avgAndMaxProducer queueProtocol.ProducerProtocolInterface) *JourneySaver {
	return &JourneySaver{consumer: consumer, accumProducer: accumProducer, avgAndMaxProducer: avgAndMaxProducer, totalPrice: 0, quantities: 0}
}

func (js *JourneySaver) saveRowsInFiles(dynMaps []*dataStructure.DynamicMap) {
	// May need optimization. Mix of memory and disk or only memory...
	log.Debugf("JourneySaver | Writing records to files")
	for _, dynMap := range dynMaps {
		stAirport, err := dynMap.GetAsString("startingAirport")
		if err != nil {
			log.Errorf("JourneySaver | Error getting starting airport | %v | Skipping row...", err)
			continue
		}
		destAirport, err := dynMap.GetAsString("destinationAirport")
		if err != nil {
			log.Errorf("JourneySaver | Error getting destination airport | %v | Skipping row...", err)
			continue
		}
		totalFare, err := dynMap.GetAsFloat("totalFare")
		if err != nil {
			log.Errorf("JourneySaver | Error getting total fare | %v | Skipping row...", err)
			continue
		}
		if totalFare <= 0 {
			log.Errorf("JourneySaver | Total fare <= 0 | Skipping row...")
			continue
		}
		journey := fmt.Sprintf("%v-%v", stAirport, destAirport)
		fileWriter, err := filemanager.NewFileWriter(journey)
		if err != nil {
			log.Errorf("JourneySaver | Error creating file writer for %v | %v | Skipping row...", journey, err)
			continue
		}
		err = fileWriter.WriteLine(fmt.Sprintf("%v\n", totalFare))
		if err != nil {
			log.Errorf("JourneySaver | Error writing total fare with file writer | %v | Skipping row...", err)
		}
		log.Debugf("JourneySaver | Added price %v to registry of journey: %v", totalFare, journey)
		if !slices.Contains(js.filesToRead, journey) {
			log.Infof("JourneySaver | Adding journey: %v to the files that must be read.", journey)
			js.filesToRead = append(js.filesToRead, journey)
		}
		err = fileWriter.FileManager.Close()
		if err != nil {
			log.Errorf("JourneySaver | Error closing file manager for journey %v | %v", journey, err)
		}
		js.totalPrice += totalFare
		js.quantities++
	}
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
func (js *JourneySaver) sendToGeneralAccumulator() error {
	dynMapData := make(map[string][]byte)
	dynMapData[utils.LocalPrice] = serializer.SerializeFloat(js.totalPrice)
	dynMapData[utils.LocalQuantity] = serializer.SerializeUint(uint32(js.quantities))
	msgToSend := &dataStructure.Message{
		TypeMessage: dataStructure.EOFFlightRows,
		DynMaps:     []*dataStructure.DynamicMap{dataStructure.NewDynamicMap(dynMapData)},
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

func (js *JourneySaver) sendAverageForJourneys(finalAvg float32) {
	for _, fileStr := range js.filesToRead {
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
		dynMap[utils.Journey] = serializer.SerializeString(fileStr)
		data := []*dataStructure.DynamicMap{dataStructure.NewDynamicMap(dynMap)}
		msg := &dataStructure.Message{
			TypeMessage: dataStructure.FlightRows,
			DynMaps:     data,
		}
		log.Debugf("JourneySaver | Sending max and avg to next step...")
		err = js.avgAndMaxProducer.Send(msg)
		if err != nil {
			log.Errorf("JourneySaver | Error sending to saver the journey %v | %v | Skipping...", fileStr, err)
			continue
		}
	}
	err := js.avgAndMaxProducer.Send(&dataStructure.Message{TypeMessage: dataStructure.EOFFlightRows})
	if err != nil {
		log.Errorf("JourneySaver | Error sending EOF to saver | %v", err)
	}
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
			log.Infof("JourneySaver | Received EOF...")
			log.Infof("JourneySaver | Sending to Gral Accum. TotalPrice: %v, Quantities: %v", js.totalPrice, js.quantities)
			err := js.sendToGeneralAccumulator()
			if err != nil {
				log.Errorf("JourneySaver | Could not send to General Accumulator. Ending execution...")
				return
			}
			log.Debugf("JourneySaver | Sent correctly!")
		} else if msg.TypeMessage == dataStructure.FlightRows {
			log.Debugf("JourneySaver | Received flight row. Now saving...")
			js.saveRowsInFiles(msg.DynMaps)
		} else if msg.TypeMessage == dataStructure.FinalAvgMsg {
			finalAvg, err := msg.DynMaps[0].GetAsFloat("finalAvg")
			if err != nil {
				log.Errorf("JourneySaver | Error getting finalAvg")
			}
			log.Infof("JourneySaver | Received Final Avg: %v. Now sending Average for Journeys...", finalAvg)
			js.sendAverageForJourneys(finalAvg)
			_, err = filemanager.MoveFiles(js.filesToRead)
			if err != nil {
				log.Errorf("JourneySaver | Error trying to move files | %v", err)
			}
			js.clearInternalState()
		}
	}
}

func (js *JourneySaver) clearInternalState() {
	js.filesToRead = []string{}
	js.totalPrice = 0
	js.quantities = 0
}
