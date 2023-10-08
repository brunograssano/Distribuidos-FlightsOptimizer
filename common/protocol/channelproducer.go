package protocol

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
)

type ProducerChannel struct {
	producerChan chan *dataStructures.Message
	totalSent    int
}

func NewProducerChannel(producerChan chan *dataStructures.Message) *ProducerChannel {
	return &ProducerChannel{
		producerChan: producerChan,
		totalSent:    0,
	}
}

func (c *ProducerChannel) Send(msg *dataStructures.Message) error {
	c.producerChan <- msg
	if msg.TypeMessage == dataStructures.FlightRows {
		c.totalSent += len(msg.DynMaps)
	}
	return nil
}

func (c *ProducerChannel) GetSentMessages() int {
	return c.totalSent
}
