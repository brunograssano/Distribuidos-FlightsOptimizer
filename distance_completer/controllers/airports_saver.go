package controllers

import (
	"distance_completer/config"
	"fmt"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	log "github.com/sirupsen/logrus"
)

type AirportSaver struct {
	c             *config.CompleterConfig
	serializer    *dataStructures.DynamicMapSerializer
	consumer      middleware.ConsumerInterface
	loadedSignals []chan bool
	fileSaver     *filemanager.FileWriter
}

func NewAirportSaver(
	conf *config.CompleterConfig,
	qMiddleware *middleware.QueueMiddleware,
	s *dataStructures.DynamicMapSerializer,
	fileLoadedSignals []chan bool,
) *AirportSaver {
	consumer := qMiddleware.CreateConsumer(conf.InputQueueAirportsName, true)
	err := consumer.BindTo(conf.ExchangeNameAirports, conf.RoutingKeyExchangeAirports)
	if err != nil {
		log.Fatalf("Error trying to bind the consumer's queue to the exchange: %v", err)
	}
	fileWriter, err := filemanager.NewFileWriter(conf.AirportsFilename)
	if err != nil {
		log.Fatalf("Error trying to initialize FileWriter in saver: %v", err)
	}
	return &AirportSaver{
		c:             conf,
		serializer:    s,
		consumer:      consumer,
		loadedSignals: fileLoadedSignals,
		fileSaver:     fileWriter,
	}
}

func (as *AirportSaver) signalCompleters() {
	for i := 0; i < len(as.loadedSignals); i++ {
		log.Infof("Sending signal to completer #%v...", i)
		as.loadedSignals[i] <- true
		close(as.loadedSignals[i])
	}
}

func (as *AirportSaver) SaveAirports() {
	for {
		msg, ok := as.consumer.Pop()
		if !ok {
			log.Infof("Closing goroutine SaverAirports")
			return
		}
		row := as.serializer.Deserialize(msg)
		if row.GetColumnCount() == 0 {
			log.Infof("Received EOF. Signalizing completers to start completion...")
			break
		}
		airportCode, err := row.GetAsString("Airport Code")
		if err != nil {
			log.Errorf("Error trying to get airport code: %v. Skipping row...", err)
			continue
		}
		lat, err := row.GetAsFloat("Latitude")
		if err != nil {
			log.Errorf("Error trying to get latitude: %v. Skipping row...", err)
			continue
		}
		long, err := row.GetAsFloat("Longitude")
		if err != nil {
			log.Errorf("Error trying to get longitude: %v. Skipping row...", err)
			continue
		}
		stringToSave := fmt.Sprintf("%v,%v,%v\n", airportCode, lat, long)
		err = as.fileSaver.WriteLine(stringToSave)
		if err != nil {
			log.Errorf("Error trying to write line: %v. Skipping row...", err)
			continue
		}
	}
	as.signalCompleters()
	err := as.fileSaver.FileManager.Close()
	if err != nil {
		log.Errorf("Error closing file...")
	}

}