package main

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func assertAvgFromChannel(t *testing.T, expectedAvg float32, consumer chan *dataStructures.Message) {
	select {
	case msg := <-consumer:
		avg, err := msg.DynMaps[0].GetAsFloat(utils.FinalAvg)

		assert.Nilf(t, err, "Got error: %v", err)
		assert.Equalf(t, expectedAvg, avg, "Different avg: %v", avg)
	case <-time.After(1 * time.Second):
		t.Errorf("Timeout! Should have finished by now...")
	}
}

func TestShouldCalculateTheAverage(t *testing.T) {
	avgCalculator := &AvgCalculator{}
	avg := avgCalculator.calculateAvg(2, 10)

	assert.Equalf(t, float32(5), avg, "Different avg: %v", avg)
}

func TestShouldReturnZeroIfTheSumIsZero(t *testing.T) {
	avgCalculator := &AvgCalculator{}
	avg := avgCalculator.calculateAvg(0, 10)

	assert.Equalf(t, float32(0), avg, "Different avg: %v", avg)
}

func TestShouldSendTheAverageToTheConsumers(t *testing.T) {
	chan1 := make(chan *dataStructures.Message, 1)
	chan2 := make(chan *dataStructures.Message, 1)
	avgCalculator := &AvgCalculator{toInternalSaversChannels: []protocol.ProducerProtocolInterface{protocol.NewProducerChannel(chan1), protocol.NewProducerChannel(chan2)}}

	go avgCalculator.sendToJourneySavers(5)

	assertAvgFromChannel(t, 5, chan1)
	assertAvgFromChannel(t, 5, chan2)
}
