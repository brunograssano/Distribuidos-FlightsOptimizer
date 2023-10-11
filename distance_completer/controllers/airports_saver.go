package controllers

import (
	"distance_completer/config"
	"fmt"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

type AirportSaver struct {
	c             *config.CompleterConfig
	consumer      middleware.ConsumerInterface
	loadedSignals []chan bool
	fileSaver     *filemanager.FileWriter
}

func NewAirportSaver(
	conf *config.CompleterConfig,
	qMiddleware *middleware.QueueMiddleware,
	fileLoadedSignals []chan bool,
) *AirportSaver {
	consumer := qMiddleware.CreateConsumer(conf.InputQueueAirportsName, true)
	err := consumer.BindTo(conf.ExchangeNameAirports, conf.RoutingKeyExchangeAirports)
	if err != nil {
		log.Fatalf("AirportsSaver | Error trying to bind the consumer's queue to the exchange | %v", err)
	}
	fileWriter, err := filemanager.NewFileWriter(conf.AirportsFilename)
	if err != nil {
		log.Fatalf("AirportsSaver | Error trying to initialize FileWriter in saver | %v", err)
	}
	return &AirportSaver{
		c:             conf,
		consumer:      consumer,
		loadedSignals: fileLoadedSignals,
		fileSaver:     fileWriter,
	}
}

func (as *AirportSaver) signalCompleters() {
	for i := 0; i < len(as.loadedSignals); i++ {
		log.Infof("AirportsSaver | Sending signal to completer #%v...", i)
		as.loadedSignals[i] <- true
		close(as.loadedSignals[i])
	}
}

func (as *AirportSaver) SaveAirports() {
	defer as.closeFile()
	for {
		msg, ok := as.consumer.Pop()
		if !ok {
			log.Infof("AirportsSaver | Closing goroutine...")
			return
		}
		msgStruct := serializer.DeserializeMsg(msg)
		log.Debugf("AirportsSaver | Received message | {type: %v, rowCount: %v}", msgStruct.TypeMessage, len(msgStruct.DynMaps))
		if msgStruct.TypeMessage == dataStructures.EOFAirports {
			log.Infof("AirportsSaver | Received EOF. Signalizing completers to start completion...")
			as.signalCompleters()
			continue
		}
		if msgStruct.TypeMessage == dataStructures.EOFFlightRows {
			log.Infof("AirportsSaver | Received EOF FlightRows. Moving airport file")
			filemanager.MoveFiles([]string{as.c.AirportsFilename})
			continue
		}
		rows := msgStruct.DynMaps
		for _, row := range rows {
			airportCode, err := row.GetAsString(utils.AirportCode)
			if err != nil {
				log.Errorf("AirportsSaver | Error trying to get airport code | %v | Skipping row...", err)
				continue
			}
			lat, err := row.GetAsFloat(utils.Latitude)
			if err != nil {
				log.Errorf("AirportsSaver | Error trying to get latitude | %v | Skipping row...", err)
				continue
			}
			long, err := row.GetAsFloat(utils.Longitude)
			if err != nil {
				log.Errorf("AirportsSaver | Error trying to get longitude | %v | Skipping row...", err)
				continue
			}
			stringToSave := fmt.Sprintf("%v,%v,%v\n", airportCode, lat, long)
			err = as.fileSaver.WriteLine(stringToSave)
			if err != nil {
				log.Errorf("AirportsSaver | Error trying to write line | %v | Skipping row...", err)
				continue
			}
		}
	}

}

func (as *AirportSaver) closeFile() {
	log.Infof("AirportsSaver | Closing file...")
	err := as.fileSaver.FileManager.Close()
	if err != nil {
		log.Errorf("AirportsSaver | Error closing airports file | %v", err)
	}
}
