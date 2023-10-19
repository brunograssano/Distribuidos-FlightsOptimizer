package main

import (
	dataStructure "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	queueProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

type AvgCalculator struct {
	toInternalSaversChannels []queueProtocol.ProducerProtocolInterface
	pricesConsumer           queueProtocol.ConsumerProtocolInterface
}

func NewAvgCalculator(toInternalSaversChannels []queueProtocol.ProducerProtocolInterface, pricesConsumer queueProtocol.ConsumerProtocolInterface) *AvgCalculator {
	return &AvgCalculator{toInternalSaversChannels: toInternalSaversChannels, pricesConsumer: pricesConsumer}
}

// CalculateAvgLoop Waits for the final results from the journey savers,
// performs the calculation, and sends the results back
func (a *AvgCalculator) CalculateAvgLoop() {
	log.Infof("AvgCalculator | Started Avg Calculator loop")
	for {
		sumOfPrices := float32(0)
		sumOfRows := 0
		log.Infof("AvgCalculator | Starting await of internalSavers | Now waiting for %v savers...", len(a.toInternalSaversChannels))
		for sentResults := 0; sentResults < len(a.toInternalSaversChannels); sentResults++ {
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

			prices, err := msg.DynMaps[0].GetAsFloat(utils.LocalPrice)
			if err != nil {
				log.Errorf("AvgCalculator | Error getting localPrice | %v", err)
				continue
			}
			sumOfPrices += prices

			rows, err := msg.DynMaps[0].GetAsInt(utils.LocalQuantity)
			if err != nil {
				log.Errorf("AvgCalculator | Error getting localQuantity | %v", err)
				continue
			}
			sumOfRows += rows

			log.Debugf("AvgCalculator | New Accum Price: %v | New Accum Count: %v", sumOfPrices, sumOfRows)
		}
		log.Infof("AvgCalculator | Received all local JourneySaver values, calculating average")
		avg := a.calculateAvg(sumOfRows, sumOfPrices)
		log.Infof("AvgCalculator | General Average is: %v | Now sending to journey savers...", avg)
		a.sendToJourneySavers(avg)
	}
}

// sendToJourneySavers Sends the average to the journey savers
func (a *AvgCalculator) sendToJourneySavers(avg float32) {
	avgBytes := serializer.SerializeFloat(avg)
	dynMap := make(map[string][]byte)
	dynMap[utils.FinalAvg] = avgBytes
	data := []*dataStructure.DynamicMap{dataStructure.NewDynamicMap(dynMap)}
	msg := &dataStructure.Message{TypeMessage: dataStructure.FinalAvgMsg, DynMaps: data}
	for i, channel := range a.toInternalSaversChannels {
		log.Infof("AvgCalculator | Sending average to saver %v", i)
		err := channel.Send(msg)
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
