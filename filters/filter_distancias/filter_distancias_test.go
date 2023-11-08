package main

import (
	"encoding/binary"
	"filters_config"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filters"
	queueProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	"math"
	"testing"
	"time"
)

type (
	mockConsumer struct {
		inputChannel chan *dataStructures.Message
		ok           bool
	}
)

func (m *mockConsumer) SetStatusOfLastMessage(b bool) {
	//TODO implement me
	panic("implement me")
}

func (m *mockConsumer) ClearData(s string) {
	return
}

func (m *mockConsumer) GetReceivedMessages(s string) int {
	return 0
}

func (m *mockConsumer) Pop() (*dataStructures.Message, bool) {
	if !m.ok {
		return &dataStructures.Message{}, m.ok
	}
	msg, ok := <-m.inputChannel
	return msg, ok
}

func (m *mockConsumer) BindTo(_ string, _ string, _ string) error {
	return nil
}

type (
	mockProducer struct {
		outputChannel chan *dataStructures.Message
	}
)

func (m *mockProducer) ClearData(s string) {
	return
}

func (m *mockProducer) GetSentMessages(s string) int {
	return 0
}

func (m *mockProducer) Send(data *dataStructures.Message) error {
	m.outputChannel <- data
	return nil
}

func TestGettingARowWithTotalDistanceGreaterThanFourTimesOfDirectDistancePassesFilter(t *testing.T) {
	input := make(chan *dataStructures.Message)
	output := make(chan *dataStructures.Message)

	mockCons := &mockConsumer{
		inputChannel: input,
		ok:           true,
	}
	arrayProducers := make([]queueProtocol.ProducerProtocolInterface, 1)
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
	row := dataStructures.NewDynamicMap(dynMap)
	input <- &dataStructures.Message{TypeMessage: dataStructures.FlightRows, DynMaps: []*dataStructures.DynamicMap{row}}
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
	input := make(chan *dataStructures.Message)
	output := make(chan *dataStructures.Message)

	mockCons := &mockConsumer{
		inputChannel: input,
		ok:           true,
	}
	arrayProducers := make([]queueProtocol.ProducerProtocolInterface, 1)
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
	row := dataStructures.NewDynamicMap(dynMap)
	msgToSend := &dataStructures.Message{TypeMessage: dataStructures.FlightRows, DynMaps: []*dataStructures.DynamicMap{row}}
	input <- msgToSend
	close(input)
	select {
	case <-output:
		t.Errorf("Should not have received this row.")
	case <-time.After(2 * time.Second):
	}
}

func TestGettingARowWithTotalDistanceLessThanFourTimesDirectDistanceShallNotPass(t *testing.T) {
	input := make(chan *dataStructures.Message)
	output := make(chan *dataStructures.Message)

	mockCons := &mockConsumer{
		inputChannel: input,
		ok:           true,
	}
	arrayProducers := make([]queueProtocol.ProducerProtocolInterface, 1)
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
	row := dataStructures.NewDynamicMap(dynMap)
	msgToSend := &dataStructures.Message{TypeMessage: dataStructures.FlightRows, DynMaps: []*dataStructures.DynamicMap{row}}
	input <- msgToSend
	close(input)
	select {
	case <-output:
		t.Errorf("Should not have received this row.")
	case <-time.After(2 * time.Second):
	}
}

func TestWithLessEqualAndGreaterForDistances(t *testing.T) {
	input := make(chan *dataStructures.Message)
	output := make(chan *dataStructures.Message)

	mockCons := &mockConsumer{
		inputChannel: input,
		ok:           true,
	}
	arrayProducers := make([]queueProtocol.ProducerProtocolInterface, 1)
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
		row := dataStructures.NewDynamicMap(dynMap)
		msgToSend := &dataStructures.Message{TypeMessage: dataStructures.FlightRows, DynMaps: []*dataStructures.DynamicMap{row}}
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
