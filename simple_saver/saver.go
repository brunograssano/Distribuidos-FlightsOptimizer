package main

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

// SimpleSaver Structure that handles the final results
type SimpleSaver struct {
	c        *SaverConfig
	consumer protocol.ConsumerProtocolInterface
	canSend  chan bool
}

// NewSimpleSaver Creates a new saver for the results
func NewSimpleSaver(qMiddleware *middleware.QueueMiddleware, c *SaverConfig, canSend chan bool) *SimpleSaver {
	consumer := protocol.NewConsumerQueueProtocolHandler(qMiddleware.CreateConsumer(c.InputQueueName, true))
	return &SimpleSaver{c: c, consumer: consumer, canSend: canSend}
}

// SaveData Saves the results from the queue in a file
func (s *SimpleSaver) SaveData() {
	for {
		msgStruct, ok := s.consumer.Pop()
		if !ok {
			log.Infof("SimpleSaver | Exiting saver")
			return
		}
		if msgStruct.TypeMessage == dataStructures.EOFFlightRows {
			log.Infof("SimpleSaver | Received all results. Closing saver...")
			s.canSend <- true
			close(s.canSend)
			return
		} else if msgStruct.TypeMessage == dataStructures.FlightRows {
			err := s.handleFlightRows(msgStruct)
			if err != nil {
				log.Errorf("SimpleSaver | Error handling flight rows. Closing saver...")
				return
			}
		}
	}
}

func (s *SimpleSaver) handleFlightRows(msgStruct *dataStructures.Message) error {
	writer, err := filemanager.NewFileWriter(s.c.OutputFileName)
	if err != nil {
		log.Errorf("SimpleSaver | Error opening file writer of output")
		return err
	}
	for _, row := range msgStruct.DynMaps {
		line := serializer.SerializeToString(row)
		log.Debugf("SimpleSaver | Saving line: %v", line)
		err = writer.WriteLine(line)
		if err != nil {
			log.Errorf("SimpleSaver | Error writing to file | %v", err)
			utils.CloseFileAndNotifyError(writer.FileManager)
			return err
		}
	}
	utils.CloseFileAndNotifyError(writer.FileManager)
	return nil
}
