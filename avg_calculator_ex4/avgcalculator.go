package main

import (
	dataStructure "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	queueProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

type AvgCalculator struct {
	toJourneySavers        []queueProtocol.ProducerProtocolInterface
	pricesConsumer         queueProtocol.ConsumerProtocolInterface
	c                      *AvgCalculatorConfig
	valuesReceivedByClient map[string]PartialSum
}

func NewAvgCalculator(toJourneySavers []queueProtocol.ProducerProtocolInterface, pricesConsumer queueProtocol.ConsumerProtocolInterface, c *AvgCalculatorConfig) *AvgCalculator {
	return &AvgCalculator{
		toJourneySavers:        toJourneySavers,
		pricesConsumer:         pricesConsumer,
		c:                      c,
		valuesReceivedByClient: make(map[string]PartialSum),
	}
}

// CalculateAvgLoop Waits for the final results from the journey savers,
// performs the calculation, and sends the results back
func (a *AvgCalculator) CalculateAvgLoop() {
	log.Infof("AvgCalculator | Started Avg Calculator loop")
	log.Infof("AvgCalculator | Starting await of internalSavers | Now waiting for %v savers...", len(a.toJourneySavers))
	for {
		msg, ok := a.pricesConsumer.Pop()
		if !ok {
			log.Errorf("AvgCalculator | Consumer closed when not expected, exiting average calculator")
			return
		}
		log.Debugf("AvgCalculator | Received message from saver")

		if msg.TypeMessage != dataStructure.EOFFlightRows {
			log.Warnf("AvgCalculator | Warning Message | Received a message that was not expected | Skipping...")
			continue
		}
		a.handleEofMsg(msg)
	}
}

func (a *AvgCalculator) handleEofMsg(msg *dataStructure.Message) {
	currPartialSum := a.getPartialSumOfClient(msg)

	prices, err := msg.DynMaps[0].GetAsFloat(utils.LocalPrice)
	if err != nil {
		log.Errorf("AvgCalculator | Error getting localPrice | %v", err)
		return
	}

	currPartialSum.sumOfPrices += prices
	rows, err := msg.DynMaps[0].GetAsInt(utils.LocalQuantity)
	if err != nil {
		log.Errorf("AvgCalculator | Error getting localQuantity | %v", err)
		return
	}
	currPartialSum.sumOfRows += rows
	currPartialSum.numOfSavers++
	a.valuesReceivedByClient[msg.ClientId] = currPartialSum

	log.Debugf("AvgCalculator | New Accum Price: %v | New Accum Count: %v", a.valuesReceivedByClient[msg.ClientId].sumOfPrices, a.valuesReceivedByClient[msg.ClientId].sumOfRows)
	if a.valuesReceivedByClient[msg.ClientId].numOfSavers == len(a.toJourneySavers) {
		a.onFinishedClientId(msg)
	}
}

func (a *AvgCalculator) getPartialSumOfClient(msg *dataStructure.Message) PartialSum {
	_, exists := a.valuesReceivedByClient[msg.ClientId]
	if !exists {
		a.valuesReceivedByClient[msg.ClientId] = PartialSum{sumOfRows: 0, sumOfPrices: 0, numOfSavers: 0}
	}
	return a.valuesReceivedByClient[msg.ClientId]
}

// sendToJourneySavers Sends the average to the journey savers
func (a *AvgCalculator) sendToJourneySavers(avg float32, msg *dataStructure.Message) {
	avgBytes := serializer.SerializeFloat(avg)
	dynMap := make(map[string][]byte)
	dynMap[utils.FinalAvg] = avgBytes
	data := []*dataStructure.DynamicMap{dataStructure.NewDynamicMap(dynMap)}

	for i, channel := range a.toJourneySavers {
		log.Infof("AvgCalculator | Sending average for client %v to saver %v", msg.ClientId, i)
		msgToSend := dataStructure.NewTypeMessageWithDataAndMsgId(dataStructure.FinalAvgMsg, msg, data, msg.MessageId+uint(i)+1)
		err := channel.Send(msgToSend)
		if err != nil {
			log.Errorf("AvgCalculator | Error sending avg | %v", err)
		}
	}
}

// calculateAvg Performs the calculation of the average
// If the total rows is zero, returns zero
func (a *AvgCalculator) calculateAvg(sumOfRows int, sumOfPrices float32) float32 {
	if sumOfRows == 0 {
		log.Warnf("AvgCalculator | Total rows is zero")
		return float32(0)
	}
	avg := sumOfPrices / float32(sumOfRows)
	log.Infof("AvgCalculator | Sum of prices: %v | Total rows: %v | Avg: %v", sumOfPrices, sumOfRows, avg)
	return avg
}

func (a *AvgCalculator) onFinishedClientId(msg *dataStructure.Message) {
	log.Infof("AvgCalculator | Received all local JourneySaver values, calculating average")
	avg := a.calculateAvg(a.valuesReceivedByClient[msg.ClientId].sumOfRows, a.valuesReceivedByClient[msg.ClientId].sumOfPrices)
	log.Infof("AvgCalculator | General Average is: %v | Now sending to journey savers...", avg)
	a.sendToJourneySavers(avg, msg)
}
