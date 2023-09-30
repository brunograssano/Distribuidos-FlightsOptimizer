package main

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	log "github.com/sirupsen/logrus"
)

type SimpleSaver struct {
	c          *SaverConfig
	consumer   middleware.ConsumerInterface
	serializer *dataStructures.DynamicMapSerializer
}

func NewSimpleSaver(qMiddleware *middleware.QueueMiddleware, c *SaverConfig, serializer *dataStructures.DynamicMapSerializer) *SimpleSaver {
	consumer := qMiddleware.CreateConsumer(c.InputQueueName, true)
	return &SimpleSaver{c: c, consumer: consumer, serializer: serializer}
}

func closeFile(file filemanager.OutputManagerInterface) {
	err := file.Close()
	if err != nil {
		log.Errorf("action: closing_file | status: error | %v", err)
	}
}

func (s *SimpleSaver) SaveData() {
	for {
		msg, ok := s.consumer.Pop()
		if !ok {
			log.Infof("Exiting saver")
			return
		}
		dynMap := s.serializer.Deserialize(msg)
		writer, err := filemanager.NewFileWriter(s.c.OutputFileName)
		if err != nil {
			return
		}
		line := s.serializer.SerializeToString(dynMap)
		err = writer.WriteLine(line)
		if err != nil {
			log.Errorf("action: writing_file | status: error | %v", err)
			closeFile(writer)
			return
		}
		closeFile(writer)
	}
}
