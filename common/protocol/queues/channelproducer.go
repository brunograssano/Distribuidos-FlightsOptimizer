package queues

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
)

type ProducerChannel struct {
	producerChan chan *dataStructures.Message
}

func NewProducerChannel(producerChan chan *dataStructures.Message) *ProducerChannel {
	return &ProducerChannel{
		producerChan: producerChan,
	}
}

func (c *ProducerChannel) Send(msg *dataStructures.Message) error {
	c.producerChan <- msg
	return nil
}
