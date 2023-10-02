package main

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

// SimpleSaver Structure that handles the final results
type SimpleSaver struct {
	c          *SaverConfig
	consumer   middleware.ConsumerInterface
	serializer *dataStructures.DynamicMapSerializer
	canSend    chan bool
}

// NewSimpleSaver Creates a new saver for the results
func NewSimpleSaver(qMiddleware *middleware.QueueMiddleware, c *SaverConfig, serializer *dataStructures.DynamicMapSerializer, canSend chan bool) *SimpleSaver {
	consumer := qMiddleware.CreateConsumer(c.InputQueueName, true)
	return &SimpleSaver{c: c, consumer: consumer, serializer: serializer, canSend: canSend}
}

// SaveData Saves the results from the queue in a file
func (s *SimpleSaver) SaveData() {
	for {
		msg, ok := s.consumer.Pop()
		if !ok {
			log.Infof("Exiting saver")
			return
		}
		dynMap := s.serializer.Deserialize(msg)
		if dynMap.GetColumnCount() == 0 {
			s.canSend <- true
			close(s.canSend)
			log.Infof("Received all results")
			return
		}
		writer, err := filemanager.NewFileWriter(s.c.OutputFileName)
		if err != nil {
			return
		}
		line := s.serializer.SerializeToString(dynMap)
		err = writer.WriteLine(line)
		if err != nil {
			log.Errorf("action: writing_file | status: error | %v", err)
			utils.CloseFileAndNotifyError(writer.FileManager)
			return
		}
		utils.CloseFileAndNotifyError(writer.FileManager)
	}
}
