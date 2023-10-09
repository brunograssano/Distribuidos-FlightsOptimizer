package main

import (
	"fmt"
	dataStructure "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"slices"
	"strconv"
)

// JourneySaver Handles the prices of the assigned journeys
type JourneySaver struct {
	consumer          protocol.ConsumerProtocolInterface
	accumProducer     protocol.ProducerProtocolInterface
	avgAndMaxProducer protocol.ProducerProtocolInterface
	filesToRead       []string
}

// NewJourneySaver Creates a new JourneySaver
func NewJourneySaver(consumer protocol.ConsumerProtocolInterface, accumProducer protocol.ProducerProtocolInterface, avgAndMaxProducer protocol.ProducerProtocolInterface) *JourneySaver {
	return &JourneySaver{consumer: consumer, accumProducer: accumProducer, avgAndMaxProducer: avgAndMaxProducer}
}

func (js *JourneySaver) saveRowsInFiles(dynMaps []*dataStructure.DynamicMap) {
	// May need optimization. Mix of memory and disk or only memory...
	log.Debugf("Writing records to files")
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
		log.Debugf("Added price %v to registry of journey: %v", totalFare, journey)
		if !slices.Contains(js.filesToRead, journey) {
			log.Infof("Adding journey: %v to the files that must be read.", journey)
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
		log.Debugf("Read price: %v", individualPrice)
		prices = append(prices, float32(individualPrice))
	}
	err = fileReader.FileManager.Close()
	if err != nil {
		log.Errorf("Error closing fileReader: %v", journeyStr)
		return prices, err
	}
	return prices, nil
}

// sumPricesAndCountQuantities Gets the values (total price and accum) of this JourneySaver by iterating all the managed files
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
		log.Infof("Finished reading %v. CurrentAccums are: {price: %v, lines: %v}", fileStr, accumPrice, accumLines)
	}
	return accumPrice, accumLines
}

// sendToGeneralAccumulator Sends to the accumulator the values that the JourneySaver managed
func (js *JourneySaver) sendToGeneralAccumulator(totalPrice float32, rows int) error {
	dynMapData := make(map[string][]byte)
	serializer := dataStructure.NewSerializer()
	dynMapData[utils.LocalPrice] = serializer.SerializeFloat(totalPrice)
	dynMapData[utils.LocalQuantity] = serializer.SerializeUint(uint32(rows))
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
	return maxVal, average
}

func (js *JourneySaver) sendAverageForJourneys(finalAvg float32) {
	serializer := dataStructure.NewSerializer()
	for _, fileStr := range js.filesToRead {
		log.Infof("JourneySaver | Reading file: %v", fileStr)
		pricesForJourney, err := js.readJourneyAsArrays(fileStr)
		if err != nil {
			log.Errorf("JourneySaver | Error reading file: %v. Skipping file...", err)
		}
		filteredPrices := js.filterGreaterThanAverage(pricesForJourney, finalAvg)
		log.Infof("JourneySaver | Filtered prices. Original len: %v ; Filtered len: %v", len(pricesForJourney), len(filteredPrices))
		journeyAverage, journeyMax := js.getMaxAndAverage(filteredPrices)
		log.Infof("JourneySaver | Average: %v, Maximum: %v", journeyAverage, journeyMax)

		dynMap := make(map[string][]byte)
		dynMap[utils.Avg] = serializer.SerializeFloat(journeyAverage)
		dynMap[utils.Max] = serializer.SerializeFloat(journeyMax)
		dynMap[utils.Journey] = serializer.SerializeString(fileStr)
		data := []*dataStructure.DynamicMap{dataStructure.NewDynamicMap(dynMap)}
		msg := &dataStructure.Message{
			TypeMessage: dataStructure.FlightRows,
			DynMaps:     data,
		}
		log.Infof("JourneySaver | Sending max and avg to next step...")
		err = js.avgAndMaxProducer.Send(msg)
		if err != nil {
			log.Errorf("JourneySaver | Error sending to saver the journey %v | %v", fileStr, err)
			return
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
			log.Infof("JourneySaver | Received EOF. Now summing prices and counting quantities to send...")
			totalPrice, quantities := js.sumPricesAndCountQuantities()
			log.Infof("JourneySaver | Sending to Gral Accum. TotalPrice: %v, Quantities: %v", totalPrice, quantities)
			err := js.sendToGeneralAccumulator(totalPrice, quantities)
			if err != nil {
				log.Errorf("JourneySaver | Could not send to General Accumulator. Ending execution...")
				return
			}
			log.Debugf("JourneySaver | Sent correctly!")
		} else if msg.TypeMessage == dataStructure.FlightRows {
			log.Debugf("JourneySaver | Received flight row. Now saving...")
			js.saveRowsInFiles(msg.DynMaps)
		} else if msg.TypeMessage == dataStructure.FinalAvg {
			finalAvg, err := msg.DynMaps[0].GetAsFloat("finalAvg")
			if err != nil {
				log.Errorf("JourneySaver | Error getting finalAvg")
			}
			log.Infof("JourneySaver | Received Final Avg: %v. Now sending Average for Journeys...", finalAvg)
			js.sendAverageForJourneys(finalAvg)
		}
	}
}
