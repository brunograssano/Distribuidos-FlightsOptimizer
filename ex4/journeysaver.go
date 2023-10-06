package main

import (
	"fmt"
	dataStructure "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	log "github.com/sirupsen/logrus"
	"slices"
	"strconv"
)

type JourneySaver struct {
	consumer          protocol.ConsumerProtocolInterface
	accumProducer     protocol.ProducerProtocolInterface
	avgAndMaxProducer protocol.ProducerProtocolInterface
	filesToRead       []string
}

func NewJourneySaver(consumer protocol.ConsumerProtocolInterface, accumProducer protocol.ProducerProtocolInterface, avgAndMaxProducer protocol.ProducerProtocolInterface) *JourneySaver {
	return &JourneySaver{consumer: consumer, accumProducer: accumProducer, avgAndMaxProducer: avgAndMaxProducer}
}

/*func (js *JourneySaver) containsJourneyFile(journey string) bool {
	for _, journeyInFile := range js.filesToRead {
		if strings.Compare(journeyInFile, journey) == 0{
			return true
		}
	}
	return false
}*/

func (js *JourneySaver) saveRowsInFiles(dynMaps []*dataStructure.DynamicMap) {
	// May need optimization. Mix of memory and disk or only memory...
	for _, dynMap := range dynMaps {
		stAirport, err := dynMap.GetAsString("startingAirport")
		if err != nil {
			log.Errorf("Error getting starting airport, skipping row...")
			continue
		}
		destAirport, err := dynMap.GetAsString("destinationAirport")
		if err != nil {
			log.Errorf("Error getting destination airport, skipping row...")
			continue
		}
		totalFare, err := dynMap.GetAsFloat("totalFare")
		if err != nil {
			log.Errorf("Error getting total fare, skipping row...")
			continue
		}
		journey := fmt.Sprintf("%v-%v", stAirport, destAirport)
		fileWriter, err := filemanager.NewFileWriter(journey)
		if err != nil {
			log.Errorf("Error creating file writer for %v. Skipping row...", journey)
			continue
		}
		err = fileWriter.WriteLine(fmt.Sprintf("%v\n", totalFare))
		if err != nil {
			log.Errorf("Error writing total fare with file writer, skipping row...")
		}
		if !slices.Contains(js.filesToRead, journey) {
			js.filesToRead = append(js.filesToRead, journey)
		}
		err = fileWriter.FileManager.Close()
		if err != nil {
			log.Errorf("Error closing file manager for journey %v...", journey)
		}
	}
}

func (js *JourneySaver) readJourney(journeyStr string) (float32, int, error) {
	fileReader, err := filemanager.NewFileReader(journeyStr)
	accumPrice := float32(0.0)
	accumLines := 0
	if err != nil {
		return 0, 0, err
	}
	for fileReader.CanRead() {
		individualPrice, err := strconv.ParseFloat(fileReader.ReadLine(), 32)
		if err != nil {
			log.Errorf("Error reading price. Skipping row...")
			continue
		}
		accumPrice += float32(individualPrice)
		accumLines++
	}
	err = fileReader.FileManager.Close()
	if err != nil {
		log.Errorf("Error closing fileReader: %v", journeyStr)
		return accumPrice, accumLines, err
	}
	return accumPrice, accumLines, nil
}

func (js *JourneySaver) readJourneyAsArrays(journeyStr string) ([]float32, error) {
	fileReader, err := filemanager.NewFileReader(journeyStr)
	var prices []float32
	for fileReader.CanRead() {
		individualPrice, err := strconv.ParseFloat(fileReader.ReadLine(), 32)
		if err != nil {
			log.Errorf("Error reading price. Skipping row...")
			continue
		}
		prices = append(prices, float32(individualPrice))
	}
	err = fileReader.FileManager.Close()
	if err != nil {
		log.Errorf("Error closing fileReader: %v", journeyStr)
		return prices, err
	}
	return prices, nil
}

func (js *JourneySaver) sumPricesAndCountQuantities() (float32, int) {
	accumPrice := float32(0.0)
	accumLines := 0
	for _, fileStr := range js.filesToRead {
		accumPriceJourney, accumLinesJourney, err := js.readJourney(fileStr)
		if err != nil {
			log.Errorf("Error reading file: %v. Skipping file...", err)
		}
		accumPrice += accumPriceJourney
		accumLines += accumLinesJourney
	}
	return accumPrice, accumLines
}

func (js *JourneySaver) sendToGeneralAccumulator(totalPrice float32, rows int) error {
	dynMapData := make(map[string][]byte)
	serializer := dataStructure.NewSerializer()
	dynMapData["localPrice"] = serializer.SerializeFloat(totalPrice)
	dynMapData["localQuantity"] = serializer.SerializeUint(uint32(rows))
	msgToSend := &dataStructure.Message{
		TypeMessage: dataStructure.EOFFlightRows,
		DynMaps:     []*dataStructure.DynamicMap{dataStructure.NewDynamicMap(dynMapData)},
	}
	err := js.accumProducer.Send(msgToSend)
	if err != nil {
		log.Errorf("Error trying to send to general accumulator: %v", err)
		return err
	}
	return nil
}

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
	return maxVal, average
}

func (js *JourneySaver) sendAverageForJourneys(finalAvg float32) {
	serializer := dataStructure.NewSerializer()
	for _, fileStr := range js.filesToRead {
		pricesForJourney, err := js.readJourneyAsArrays(fileStr)
		if err != nil {
			log.Errorf("Error reading file: %v. Skipping file...", err)
		}
		filteredPrices := js.filterGreaterThanAverage(pricesForJourney, finalAvg)
		journeyAverage, journeyMax := js.getMaxAndAverage(filteredPrices)

		dynMap := make(map[string][]byte)
		dynMap["avg"] = serializer.SerializeFloat(journeyAverage)
		dynMap["max"] = serializer.SerializeFloat(journeyMax)
		data := []*dataStructure.DynamicMap{dataStructure.NewDynamicMap(dynMap)}
		msg := &dataStructure.Message{
			TypeMessage: dataStructure.FinalAvg,
			DynMaps:     data,
		}

		err = js.avgAndMaxProducer.Send(msg)
		if err != nil {
			log.Errorf("Error sending to saver the journey %v", fileStr)
		}
	}
}

func (js *JourneySaver) SavePricesForJourneys() {
	for {
		msg, ok := js.consumer.Pop()
		if !ok {
			log.Errorf("Input of messages closed. Ending execution...")
			return
		}
		if msg.TypeMessage == dataStructure.EOFFlightRows {
			totalPrice, quantities := js.sumPricesAndCountQuantities()
			err := js.sendToGeneralAccumulator(totalPrice, quantities)
			if err != nil {
				log.Errorf("Could not send to General Accumulator. Ending execution...")
				return
			}
		} else if msg.TypeMessage == dataStructure.FlightRows {
			js.saveRowsInFiles(msg.DynMaps)
		} else if msg.TypeMessage == dataStructure.FinalAvg {
			finalAvg, err := msg.DynMaps[0].GetAsFloat("finalAvg")
			if err != nil {
				log.Errorf("Error getting finalAvg")
			}
			js.sendAverageForJourneys(finalAvg)
		}
	}
}
