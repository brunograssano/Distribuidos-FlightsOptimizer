package controllers

import (
	"distance_completer/config"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

type (
	mockConsumer struct {
		inputChannel chan *dataStructures.Message
		ok           bool
	}
)

func (m *mockConsumer) ClearData() {

}

func (m *mockConsumer) GetReceivedMessages() int {
	return 0
}

func (m *mockConsumer) Pop() (*dataStructures.Message, bool) {
	if !m.ok {
		return &dataStructures.Message{}, m.ok
	}
	msg, ok := <-m.inputChannel
	return msg, ok
}

type (
	mockProducer struct {
		outputChannel chan *dataStructures.Message
	}
)

func (m *mockProducer) ClearData() {
	return
}

func (m *mockProducer) GetSentMessages() int {
	return 0
}

func (m *mockProducer) Send(data *dataStructures.Message) error {
	m.outputChannel <- data
	return nil
}

func TestCompleteDistancesForAFlightThatHasTwoStopoversSatisfiesGeneralConditions(t *testing.T) {
	input := make(chan *dataStructures.Message)
	output := make(chan *dataStructures.Message)
	mapAirports := make(map[string][2]float32)
	mapAirports["A"] = [2]float32{0.0, 0.0}
	mapAirports["B"] = [2]float32{1.0, 0.0}
	mapAirports["C"] = [2]float32{0.0, 1.0}
	mapAirports["D"] = [2]float32{1.0, 1.0}
	mockCons := &mockConsumer{
		inputChannel: input,
		ok:           true,
	}
	mockProd := &mockProducer{
		outputChannel: output,
	}
	signalChan := make(chan string)
	distCompleter := &DistanceCompleter{
		completerId:      0,
		airportsMap:      mapAirports,
		c:                &config.CompleterConfig{},
		consumer:         mockCons,
		producer:         mockProd,
		fileLoadedSignal: signalChan,
	}
	go distCompleter.CompleteDistances()
	dynMapWithRoute := make(map[string][]byte)
	dynMapWithRoute[utils.StartingAirport] = []byte("A")
	dynMapWithRoute[utils.DestinationAirport] = []byte("D")
	dynMapWithRoute[utils.Route] = []byte("A||B||C||D")
	dynMapStructure := dataStructures.NewDynamicMap(dynMapWithRoute)
	signalChan <- ""
	close(signalChan)
	input <- &dataStructures.Message{TypeMessage: dataStructures.FlightRows, DynMaps: []*dataStructures.DynamicMap{dynMapStructure}}
	dynMapResult := (<-output).DynMaps[0]

	assert.Equalf(t, uint32(5), dynMapResult.GetColumnCount(), "Column count was %v, and expected was 5", dynMapResult.GetColumnCount())

	ttDistance, err := dynMapResult.GetAsFloat(utils.TotalTravelDistance)
	assert.Nilf(t, err, "Error getting total travel distance: %v", err)

	dDistance, err := dynMapResult.GetAsFloat(utils.DirectDistance)
	assert.Nilf(t, err, "Error getting direct distance: %v", err)

	assert.Lessf(t, dDistance, ttDistance, "directDistance should be less than totalTravelDistance. DDistance = %v, TTDistance = %v", dDistance, ttDistance)
}

func TestDirectDistanceShouldBeSameAsTotalTravelDistance(t *testing.T) {
	input := make(chan *dataStructures.Message)
	output := make(chan *dataStructures.Message)
	mapAirports := make(map[string][2]float32)
	mapAirports["A"] = [2]float32{0.0, 0.0}
	mapAirports["B"] = [2]float32{0.0, 1.0}
	mapAirports["C"] = [2]float32{0.0, 2.0}
	mapAirports["D"] = [2]float32{0.0, 3.0}
	mockCons := &mockConsumer{
		inputChannel: input,
		ok:           true,
	}
	mockProd := &mockProducer{
		outputChannel: output,
	}
	signalChan := make(chan string)
	distCompleter := &DistanceCompleter{
		completerId:      0,
		airportsMap:      mapAirports,
		c:                &config.CompleterConfig{},
		consumer:         mockCons,
		producer:         mockProd,
		fileLoadedSignal: signalChan,
	}
	go distCompleter.CompleteDistances()
	dynMapWithRoute := make(map[string][]byte)
	dynMapWithRoute[utils.StartingAirport] = []byte("A")
	dynMapWithRoute[utils.DestinationAirport] = []byte("D")
	dynMapWithRoute[utils.Route] = []byte("A||B||C||D")
	dynMapStructure := dataStructures.NewDynamicMap(dynMapWithRoute)
	signalChan <- ""
	close(signalChan)
	input <- &dataStructures.Message{TypeMessage: dataStructures.FlightRows, DynMaps: []*dataStructures.DynamicMap{dynMapStructure}}
	dynMapResult := (<-output).DynMaps[0]

	assert.Equalf(t, uint32(5), dynMapResult.GetColumnCount(), "Column count was %v, and expected was 5", dynMapResult.GetColumnCount())

	ttDistance, err := dynMapResult.GetAsFloat(utils.TotalTravelDistance)
	assert.Nilf(t, err, "Error getting total travel distance: %v", err)

	dDistance, err := dynMapResult.GetAsFloat(utils.DirectDistance)
	assert.Nilf(t, err, "Error getting direct distance: %v", err)

	assert.Truef(t, math.Abs(float64(dDistance-ttDistance)) < 0.001,
		"totalTravelDistance should be equal to directDistance. DDistance = %v, TTDistance = %v", dDistance, ttDistance)

}

func TestTotalTravelDistanceShouldBeThreeTimesTheDirectDistance(t *testing.T) {
	input := make(chan *dataStructures.Message)
	output := make(chan *dataStructures.Message)
	mapAirports := make(map[string][2]float32)
	mapAirports["A"] = [2]float32{0.0, 0.0}
	mapAirports["B"] = [2]float32{0.0, 1.0}
	mapAirports["C"] = [2]float32{1.0, 1.0}
	mapAirports["D"] = [2]float32{0.0, 1.0}
	mockCons := &mockConsumer{
		inputChannel: input,
		ok:           true,
	}
	mockProd := &mockProducer{
		outputChannel: output,
	}
	signalChan := make(chan string)
	distCompleter := &DistanceCompleter{
		completerId:      0,
		airportsMap:      mapAirports,
		c:                &config.CompleterConfig{},
		consumer:         mockCons,
		producer:         mockProd,
		fileLoadedSignal: signalChan,
	}
	go distCompleter.CompleteDistances()
	dynMapWithRoute := make(map[string][]byte)
	dynMapWithRoute[utils.StartingAirport] = []byte("A")
	dynMapWithRoute[utils.DestinationAirport] = []byte("D")
	dynMapWithRoute[utils.Route] = []byte("A||B||C||D")
	dynMapStructure := dataStructures.NewDynamicMap(dynMapWithRoute)
	signalChan <- ""
	close(signalChan)
	input <- &dataStructures.Message{TypeMessage: dataStructures.FlightRows, DynMaps: []*dataStructures.DynamicMap{dynMapStructure}}
	dynMapResult := (<-output).DynMaps[0]

	assert.Equalf(t, uint32(5), dynMapResult.GetColumnCount(), "Column count was %v, and expected was 5", dynMapResult.GetColumnCount())

	ttDistance, err := dynMapResult.GetAsFloat(utils.TotalTravelDistance)
	assert.Nilf(t, err, "Error getting total travel distance: %v", err)

	dDistance, err := dynMapResult.GetAsFloat(utils.DirectDistance)
	assert.Nilf(t, err, "Error getting direct distance: %v", err)

	assert.Truef(t, math.Abs(float64(3*dDistance-ttDistance)) < 0.001,
		"totalTravelDistance should be equal to three times the directDistance. 3*DDistance = %v, DDistance = %v, TTDistance = %v", dDistance*3, dDistance, ttDistance)

}
