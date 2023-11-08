package queues

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	log "github.com/sirupsen/logrus"
)

type ConsumerChannel struct {
	consumerChan      chan *dataStructures.Message
	recvCountByClient map[string]int
}

func NewConsumerChannel(consumerChan chan *dataStructures.Message) *ConsumerChannel {
	return &ConsumerChannel{
		consumerChan:      consumerChan,
		recvCountByClient: make(map[string]int),
	}
}

func (c *ConsumerChannel) Pop() (*dataStructures.Message, bool) {
	msg, ok := <-c.consumerChan
	if ok {
		if msg.TypeMessage == dataStructures.FlightRows {
			_, exists := c.recvCountByClient[msg.ClientId]
			if !exists {
				c.recvCountByClient[msg.ClientId] = 0
			}
			c.recvCountByClient[msg.ClientId] += len(msg.DynMaps)
		}
	}
	return msg, ok
}

func (c *ConsumerChannel) GetReceivedMessages(clientId string) int {
	count, exists := c.recvCountByClient[clientId]
	if !exists {
		log.Warnf("ConsumerChannel | Warning Message | Client Id not found. Returning 0 for received messages.")
		return 0
	}
	return count
}

func (c *ConsumerChannel) ClearData(clientId string) {
	delete(c.recvCountByClient, clientId)
}

func (c *ConsumerChannel) SetStatusOfLastMessage(status bool) {
}
