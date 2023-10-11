package main

import (
	"encoding/binary"
	"filters_config"
	"github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filters"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	"math"
	"testing"
	"time"
)

type (
	mockConsumer struct {
		inputChannel chan *data_structures.Message
		ok           bool
	}
)

func (m *mockConsumer) ClearData() {
	return
}

func (m *mockConsumer) GetReceivedMessages() int {
	return 0
}

func (m *mockConsumer) Pop() (*data_structures.Message, bool) {
	if !m.ok {
		return &data_structures.Message{}, m.ok
	}
	msg, ok := <-m.inputChannel
	return msg, ok
}

func (m *mockConsumer) BindTo(_ string, _ string) error {
	return nil
}

type (
	mockProducer struct {
		outputChannel chan *data_structures.Message
	}
)

func (m *mockProducer) ClearData() {
	return
}

func (m *mockProducer) GetSentMessages() int {
	return 0
}

func (m *mockProducer) Send(data *data_structures.Message) error {
	m.outputChannel <- data
	return nil
}

func TestGettingARowWithTotalDistanceGreaterThanFourTimesOfDirectDistancePassesFilter(t *testing.T) {
	input := make(chan *data_structures.Message)
	output := make(chan *data_structures.Message)

	mockCons := &mockConsumer{
		inputChannel: input,
		ok:           true,
	}
	arrayProducers := make([]protocol.ProducerProtocolInterface, 1)
	arrayProducers[0] = &mockProducer{
		outputChannel: output,
	}
	filterDistancias := &FilterDistances{
		filterId:  0,
		config:    &filters_config.FilterConfig{},
		consumer:  mockCons,
		producers: arrayProducers,
		filter:    filters.NewFilter(),
	}
	go filterDistancias.FilterDistances()

	dynMap := make(map[string][]byte)
	dynMap["directDistance"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["directDistance"], math.Float32bits(1.9))
	dynMap["totalTravelDistance"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["totalTravelDistance"], math.Float32bits(8.5))
	row := data_structures.NewDynamicMap(dynMap)
	input <- &data_structures.Message{TypeMessage: data_structures.FlightRows, DynMaps: []*data_structures.DynamicMap{row}}
	close(input)
	select {
	case msg := <-output:
		if len(msg.DynMaps) != 1 {
			t.Errorf("Received a row that was not expected, it has %v rows", len(msg.DynMaps))
		}
		columnCount := msg.DynMaps[0].GetColumnCount()
		if columnCount != 2 {
			t.Errorf("Received a row that was not expected, has not 2 columns, it has %v", columnCount)
		}
	case <-time.After(2 * time.Second):
		t.Errorf("Timeout happened. Should have finished before.")
	}
}

func TestGettingARowWithTotalDistanceEqualToFourTimesDirectDistanceShallNotPass(t *testing.T) {
	input := make(chan *data_structures.Message)
	output := make(chan *data_structures.Message)

	mockCons := &mockConsumer{
		inputChannel: input,
		ok:           true,
	}
	arrayProducers := make([]protocol.ProducerProtocolInterface, 1)
	arrayProducers[0] = &mockProducer{
		outputChannel: output,
	}
	filterDistancias := &FilterDistances{
		filterId:  0,
		config:    &filters_config.FilterConfig{},
		consumer:  mockCons,
		producers: arrayProducers,
		filter:    filters.NewFilter(),
	}
	go filterDistancias.FilterDistances()

	dynMap := make(map[string][]byte)
	dynMap["directDistance"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["directDistance"], math.Float32bits(1.9))
	dynMap["totalTravelDistance"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["totalTravelDistance"], math.Float32bits(7.6))
	row := data_structures.NewDynamicMap(dynMap)
	msgToSend := &data_structures.Message{TypeMessage: data_structures.FlightRows, DynMaps: []*data_structures.DynamicMap{row}}
	input <- msgToSend
	close(input)
	select {
	case <-output:
		t.Errorf("Should not have received this row.")
	case <-time.After(2 * time.Second):
	}
}

func TestGettingARowWithTotalDistanceLessThanFourTimesDirectDistanceShallNotPass(t *testing.T) {
	input := make(chan *data_structures.Message)
	output := make(chan *data_structures.Message)

	mockCons := &mockConsumer{
		inputChannel: input,
		ok:           true,
	}
	arrayProducers := make([]protocol.ProducerProtocolInterface, 1)
	arrayProducers[0] = &mockProducer{
		outputChannel: output,
	}
	filterDistancias := &FilterDistances{
		filterId:  0,
		config:    &filters_config.FilterConfig{},
		consumer:  mockCons,
		producers: arrayProducers,
		filter:    filters.NewFilter(),
	}
	go filterDistancias.FilterDistances()

	dynMap := make(map[string][]byte)
	dynMap["directDistance"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["directDistance"], math.Float32bits(1.9))
	dynMap["totalTravelDistance"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["totalTravelDistance"], math.Float32bits(5.0))
	row := data_structures.NewDynamicMap(dynMap)
	msgToSend := &data_structures.Message{TypeMessage: data_structures.FlightRows, DynMaps: []*data_structures.DynamicMap{row}}
	input <- msgToSend
	close(input)
	select {
	case <-output:
		t.Errorf("Should not have received this row.")
	case <-time.After(2 * time.Second):
	}
}

func TestWithLessEqualAndGreaterForDistances(t *testing.T) {
	input := make(chan *data_structures.Message)
	output := make(chan *data_structures.Message)

	mockCons := &mockConsumer{
		inputChannel: input,
		ok:           true,
	}
	arrayProducers := make([]protocol.ProducerProtocolInterface, 1)
	arrayProducers[0] = &mockProducer{
		outputChannel: output,
	}
	filterDistancias := &FilterDistances{
		filterId:  0,
		config:    &filters_config.FilterConfig{},
		consumer:  mockCons,
		producers: arrayProducers,
		filter:    filters.NewFilter(),
	}
	go filterDistancias.FilterDistances()

	for i := 0; i < 3; i++ {
		dynMap := make(map[string][]byte)
		dynMap["directDistance"] = make([]byte, 4)
		binary.BigEndian.PutUint32(dynMap["directDistance"], math.Float32bits(1.9))
		dynMap["totalTravelDistance"] = make([]byte, 4)
		binary.BigEndian.PutUint32(dynMap["totalTravelDistance"], math.Float32bits(5.7+1.9*float32(i)))
		row := data_structures.NewDynamicMap(dynMap)
		msgToSend := &data_structures.Message{TypeMessage: data_structures.FlightRows, DynMaps: []*data_structures.DynamicMap{row}}
		input <- msgToSend
	}
	close(input)
	rowCountRecvd := 0
	for i := 0; i < 3; i++ {
		select {
		case result := <-output:
			newRow := result.DynMaps[0]
			rowCountRecvd++
			colC := newRow.GetColumnCount()
			if colC != 2 {
				t.Errorf("Received a row that was not expected, has less than 3 stopovers...")
			}

		case <-time.After(1 * time.Second):
			// Should only read one message from channel (The greater one)
			if rowCountRecvd != 1 {
				t.Errorf("Timeout! Should have finished by now...")
			}
		}
	}
	if rowCountRecvd != 1 {
		t.Errorf("Expected to receive only 1 row, but %v were received", rowCountRecvd)
	}
}
