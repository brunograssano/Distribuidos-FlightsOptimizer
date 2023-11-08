package queues

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	log "github.com/sirupsen/logrus"
)

type ProducerChannel struct {
	producerChan      chan *dataStructures.Message
	totalSentByClient map[string]int
}

func NewProducerChannel(producerChan chan *dataStructures.Message) *ProducerChannel {
	return &ProducerChannel{
		producerChan:      producerChan,
		totalSentByClient: make(map[string]int),
	}
}

func (c *ProducerChannel) Send(msg *dataStructures.Message) error {
	c.producerChan <- msg
	if msg.TypeMessage == dataStructures.FlightRows {
		_, exists := c.totalSentByClient[msg.ClientId]
		if !exists {
			c.totalSentByClient[msg.ClientId] = 0
		}
		c.totalSentByClient[msg.ClientId] += len(msg.DynMaps)
	}
	return nil
}

func (c *ProducerChannel) GetSentMessages(clientId string) int {
	count, exists := c.totalSentByClient[clientId]
	if !exists {
		log.Warnf("ProducerQueueProtocolHandler | Warning Message | Client Id not found. Returning 0 for sent messages.")
		return 0
	}
	return count
}

func (c *ProducerChannel) ClearData(clientId string) {
	delete(c.totalSentByClient, clientId)
}
