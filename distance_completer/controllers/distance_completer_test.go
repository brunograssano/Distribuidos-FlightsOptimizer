package controllers

import (
	"distance_completer/config"
	"github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"math"
	"testing"
)

type (
	mockConsumer struct {
		inputChannel chan []byte
		ok           bool
	}
)

func (m *mockConsumer) Pop() ([]byte, bool) {
	if !m.ok {
		return []byte{}, m.ok
	}
	msg, ok := <-m.inputChannel
	return msg, ok
}

func (m *mockConsumer) BindTo(_ string, _ string) error {
	return nil
}

type (
	mockProducer struct {
		outputChannel chan []byte
	}
)

func (m *mockProducer) Send(data []byte) error {
	m.outputChannel <- data
	return nil
}

func TestCompleteDistancesForAFlightThatHasTwoStopoversSatisfiesGeneralConditions(t *testing.T) {
	input := make(chan []byte)
	output := make(chan []byte)
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
	signalChan := make(chan bool)
	distCompleter := &DistanceCompleter{
		completerId:      0,
		airportsMap:      mapAirports,
		c:                &config.CompleterConfig{},
		consumer:         mockCons,
		producer:         mockProd,
		serializer:       data_structures.NewDynamicMapSerializer(),
		fileLoadedSignal: signalChan,
	}
	go distCompleter.CompleteDistances()
	dynMapWithRoute := make(map[string][]byte)
	dynMapWithRoute["startingAirport"] = []byte("A")
	dynMapWithRoute["destinationAirport"] = []byte("D")
	dynMapWithRoute["route"] = []byte("A||B||C||D")
	dynMapStructure := data_structures.NewDynamicMap(dynMapWithRoute)
	serializer := data_structures.NewDynamicMapSerializer()
	signalChan <- true
	close(signalChan)
	input <- serializer.Serialize(dynMapStructure)
	dynMapResult := serializer.Deserialize(<-output)
	if dynMapResult.GetColumnCount() != 5 {
		t.Errorf("Column count was %v, and expected was 5", dynMapResult.GetColumnCount())
	}
	ttDistance, err := dynMapResult.GetAsFloat("totalTravelDistance")
	if err != nil {
		t.Errorf("Error getting total travel distance: %v", err)
	}
	dDistance, err := dynMapResult.GetAsFloat("directDistance")
	if err != nil {
		t.Errorf("Error getting direct distance: %v", err)
	}
	if dDistance > ttDistance {
		t.Errorf("totalTravelDistance should be less than directDistance. DDistance = %v, TTDistance = %v", dDistance, ttDistance)
	}
}

func TestDirectDistanceShouldBeSameAsTotalTravelDistance(t *testing.T) {
	input := make(chan []byte)
	output := make(chan []byte)
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
	signalChan := make(chan bool)
	distCompleter := &DistanceCompleter{
		completerId:      0,
		airportsMap:      mapAirports,
		c:                &config.CompleterConfig{},
		consumer:         mockCons,
		producer:         mockProd,
		serializer:       data_structures.NewDynamicMapSerializer(),
		fileLoadedSignal: signalChan,
	}
	go distCompleter.CompleteDistances()
	dynMapWithRoute := make(map[string][]byte)
	dynMapWithRoute["startingAirport"] = []byte("A")
	dynMapWithRoute["destinationAirport"] = []byte("D")
	dynMapWithRoute["route"] = []byte("A||B||C||D")
	dynMapStructure := data_structures.NewDynamicMap(dynMapWithRoute)
	serializer := data_structures.NewDynamicMapSerializer()
	signalChan <- true
	close(signalChan)
	input <- serializer.Serialize(dynMapStructure)
	dynMapResult := serializer.Deserialize(<-output)
	if dynMapResult.GetColumnCount() != 5 {
		t.Errorf("Column count was %v, and expected was 5", dynMapResult.GetColumnCount())
	}
	ttDistance, err := dynMapResult.GetAsFloat("totalTravelDistance")
	if err != nil {
		t.Errorf("Error getting total travel distance: %v", err)
	}
	dDistance, err := dynMapResult.GetAsFloat("directDistance")
	if err != nil {
		t.Errorf("Error getting direct distance: %v", err)
	}
	if math.Abs(float64(dDistance-ttDistance)) > 0.001 {
		t.Errorf("totalTravelDistance should be equal to directDistance. DDistance = %v, TTDistance = %v", dDistance, ttDistance)
	}
}

func TestTotalTravelDistanceShouldBeThreeTimesTheDirectDistance(t *testing.T) {
	input := make(chan []byte)
	output := make(chan []byte)
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
	signalChan := make(chan bool)
	distCompleter := &DistanceCompleter{
		completerId:      0,
		airportsMap:      mapAirports,
		c:                &config.CompleterConfig{},
		consumer:         mockCons,
		producer:         mockProd,
		serializer:       data_structures.NewDynamicMapSerializer(),
		fileLoadedSignal: signalChan,
	}
	go distCompleter.CompleteDistances()
	dynMapWithRoute := make(map[string][]byte)
	dynMapWithRoute["startingAirport"] = []byte("A")
	dynMapWithRoute["destinationAirport"] = []byte("D")
	dynMapWithRoute["route"] = []byte("A||B||C||D")
	dynMapStructure := data_structures.NewDynamicMap(dynMapWithRoute)
	serializer := data_structures.NewDynamicMapSerializer()
	signalChan <- true
	close(signalChan)
	input <- serializer.Serialize(dynMapStructure)
	dynMapResult := serializer.Deserialize(<-output)
	if dynMapResult.GetColumnCount() != 5 {
		t.Errorf("Column count was %v, and expected was 5", dynMapResult.GetColumnCount())
	}
	ttDistance, err := dynMapResult.GetAsFloat("totalTravelDistance")
	if err != nil {
		t.Errorf("Error getting total travel distance: %v", err)
	}
	dDistance, err := dynMapResult.GetAsFloat("directDistance")
	if err != nil {
		t.Errorf("Error getting direct distance: %v", err)
	}
	if math.Abs(float64(3*dDistance-ttDistance)) > 0.001 {
		t.Errorf("totalTravelDistance should be equal to three times the directDistance. 3*DDistance = %v, DDistance = %v, TTDistance = %v", dDistance*3, dDistance, ttDistance)
	}
}