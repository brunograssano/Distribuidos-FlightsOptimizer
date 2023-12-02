package saver

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	queueProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	"github.com/brunograssano/Distribuidos-TP1/common/queuefactory"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

const saverId = 0

// SimpleSaver Structure that handles the final results
type SimpleSaver struct {
	c            *Config
	consumer     queueProtocol.ConsumerProtocolInterface
	checkpointer *checkpointer.CheckpointerHandler
}

// NewSimpleSaver Creates a new saver for the results
func NewSimpleSaver(
	qFactory queuefactory.QueueProtocolFactory,
	c *Config,
	chkHandler *checkpointer.CheckpointerHandler,
) *SimpleSaver {
	consumer := qFactory.CreateConsumer(fmt.Sprintf("%v-%v", c.InputQueueName, c.ID))
	chkHandler.AddCheckpointable(consumer, saverId)
	return &SimpleSaver{c: c, consumer: consumer, checkpointer: chkHandler}
}

// SaveData Saves the results from the queue in a file
func (s *SimpleSaver) SaveData() {
	log.Infof("SimpleSaver | Goroutine started")
	for {
		msg, ok := s.consumer.Pop()
		if !ok {
			log.Infof("SimpleSaver | Exiting saver")
			return
		}
		if msg.TypeMessage == dataStructures.EOFFlightRows {
			log.Infof("SimpleSaver | Received all results from client %v. Renaming file saved...", msg.ClientId)
			err := filemanager.MoveFiles([]string{fmt.Sprintf("%v_%v.csv", s.c.OutputFileName, msg.ClientId)}, msg.ClientId)
			if err != nil {
				log.Errorf("SimpleSaver | Error moving to file to folder | %v", err)
			}
		} else if msg.TypeMessage == dataStructures.FlightRows {
			err := s.handleFlightRows(msg)
			if err != nil {
				log.Errorf("SimpleSaver | Error handling flight rows. Closing saver...")
				return
			}
		}
		err := s.checkpointer.DoCheckpoint(saverId)
		if err != nil {
			log.Errorf("SimpleSaver | Error on checkpointing | %v", err)
		}
	}
}

func (s *SimpleSaver) handleFlightRows(msg *dataStructures.Message) error {
	if filemanager.DirectoryExists(msg.ClientId) {
		log.Warnf("SimpleSaver | Already processed client %v, discarding message...", msg.ClientId)
		return nil
	}
	file := fmt.Sprintf("%v_%v.csv", s.c.OutputFileName, msg.ClientId)
	writer, err := filemanager.NewFileWriter(file)
	if err != nil {
		log.Errorf("SimpleSaver | Error opening file writer of output")
		return err
	}
	defer utils.CloseFileAndNotifyError(writer.FileManager)
	return s.writeRowsToFile(msg.DynMaps, writer)
}

func (s *SimpleSaver) writeRowsToFile(rows []*dataStructures.DynamicMap, writer filemanager.OutputManagerInterface) error {
	for _, row := range rows {
		line := serializer.SerializeToString(row)
		log.Debugf("SimpleSaver | Saving line: %v", line)
		err := writer.WriteLine(line)
		if err != nil {
			log.Errorf("SimpleSaver | Error writing to file | %v", err)
			return err
		}
	}
	return nil
}
