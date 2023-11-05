package controllers

import (
	"distance_completer/config"
	"fmt"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"strings"
)

type AirportSaver struct {
	c          *config.CompleterConfig
	consumer   queues.ConsumerProtocolInterface
	fileSavers map[string]*filemanager.FileWriter
}

func NewAirportSaver(
	conf *config.CompleterConfig,
	qMiddleware *middleware.QueueMiddleware,
) *AirportSaver {
	consumer := qMiddleware.CreateConsumer(fmt.Sprintf("%v-%v", conf.InputQueueAirportsName, conf.ID), true)
	err := consumer.BindTo(conf.ExchangeNameAirports, conf.RoutingKeyExchangeAirports, "fanout")
	if err != nil {
		log.Fatalf("AirportsSaver | Error trying to bind the consumer's queue to the exchange | %v", err)
	}
	return &AirportSaver{
		c:          conf,
		consumer:   queues.NewConsumerQueueProtocolHandler(consumer),
		fileSavers: make(map[string]*filemanager.FileWriter),
	}
}

func (as *AirportSaver) createFileWriter(clientId string) *filemanager.FileWriter {
	_, exists := as.fileSavers[clientId]
	if exists {
		log.Warnf("AirportsSaver | Error opening new file writer | The same file writer does already exist | Avoiding its creation")
		return nil
	}
	fileWriter, err := filemanager.NewFileWriter(as.c.AirportsFilename + "_" + clientId + utils.TempSuffix + utils.CsvSuffix)
	if err != nil {
		log.Fatalf("AirportsSaver | Error trying to initialize FileWriter in saver | %v", err)
	}
	as.fileSavers[clientId] = fileWriter
	return fileWriter
}

func (as *AirportSaver) markFileAsDone(clientId string) error {
	fileWriter, exists := as.fileSavers[clientId]
	if !exists {
		log.Errorf("AirportsSaver | Error trying to mark files a done | The file does not exist.")
		return fmt.Errorf("file does not exist")
	}
	err := fileWriter.FileManager.Close()
	if err != nil {
		log.Errorf("AirportsSaver | Error trying to close file | %v", err)
		return err
	}
	delete(as.fileSavers, clientId)
	err = filemanager.RenameFile(
		as.c.AirportsFilename+"_"+clientId+utils.TempSuffix+utils.CsvSuffix,
		as.c.AirportsFilename+"_"+clientId+utils.CsvSuffix,
	)
	log.Infof("AirportsSaver | Marked file for client id %v as finished", clientId)
	return nil
}

func (as *AirportSaver) SaveAirports() {
	for {
		msg, ok := as.consumer.Pop()
		if !ok {
			log.Infof("AirportsSaver | Closing goroutine...")
			return
		}
		log.Debugf("AirportsSaver | Received message | {type: %v, rowCount: %v}", msg.TypeMessage, len(msg.DynMaps))
		if msg.TypeMessage == dataStructures.EOFAirports {
			as.handleAirportsEOF(msg)
		} else if msg.TypeMessage == dataStructures.Airports {
			as.handleAirports(msg)
		} else {
			log.Warnf("AirportsSaver | Received Unknown Type of Message | Type was: %v", msg.TypeMessage)
		}
	}
}

func (as *AirportSaver) handleAirports(msgStruct *dataStructures.Message) {
	fileSaver, existsFS := as.fileSavers[msgStruct.ClientId]
	if !existsFS {
		fileSaver = as.createFileWriter(msgStruct.ClientId)
	}
	rows := msgStruct.DynMaps
	stringToSave := as.getLineToSave(rows)
	err := fileSaver.WriteLine(stringToSave)
	if err != nil {
		log.Errorf("AirportsSaver | Error trying to save airports | %v", err)
	}
}

func (as *AirportSaver) handleAirportsEOF(msgStruct *dataStructures.Message) {
	log.Infof("AirportsSaver | Received EOF. Signalizing completers to start completion...")
	err := as.markFileAsDone(msgStruct.ClientId)
	if err != nil {
		log.Errorf("AirportsSaver | Error marking file as done | %v", err)
	}
}

func (as *AirportSaver) getLineToSave(rows []*dataStructures.DynamicMap) string {
	var stringToSave strings.Builder
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
		stringToSave.WriteString(fmt.Sprintf("%v,%v,%v\n", airportCode, lat, long))
		if err != nil {
			log.Errorf("AirportsSaver | Error trying to write line | %v | Skipping row...", err)
			continue
		}
	}
	return stringToSave.String()
}
