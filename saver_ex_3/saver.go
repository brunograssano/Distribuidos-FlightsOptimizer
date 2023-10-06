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
	serializer *dataStructures.Serializer
	canSend    chan bool
}

// NewSimpleSaver Creates a new saver for the results
func NewSimpleSaver(qMiddleware *middleware.QueueMiddleware, c *SaverConfig, serializer *dataStructures.Serializer, canSend chan bool) *SimpleSaver {
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
		msgStruct := s.serializer.DeserializeMsg(msg)
		writer, err := filemanager.NewFileWriter(s.c.OutputFileNames[0])
		if err != nil {
			return
		}
		for _, row := range msgStruct.DynMaps {
			// todo cambiar a EOF msg
			if row.GetColumnCount() == 0 {
				s.canSend <- true
				close(s.canSend)
				log.Infof("Received all results")
				return
			}

			line := s.serializer.SerializeToString(row)
			err = writer.WriteLine(line)
			if err != nil {
				log.Errorf("action: writing_file | status: error | %v", err)
				utils.CloseFileAndNotifyError(writer.FileManager)
				return
			}
		}
		utils.CloseFileAndNotifyError(writer.FileManager)
	}
}
