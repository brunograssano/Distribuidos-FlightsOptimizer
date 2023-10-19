package main

import (
	"encoding/binary"
	"filters_config"
	"github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filters"
	queueProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
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

func TestGettingARowWithTotalStopoversLessThanThreeShouldNotSendIt(t *testing.T) {
	input := make(chan *data_structures.Message)
	output := make(chan *data_structures.Message)

	mockCons := &mockConsumer{
		inputChannel: input,
		ok:           true,
	}
	arrayProducers := make([]queueProtocol.ProducerProtocolInterface, 1)
	arrayProducers[0] = &mockProducer{
		outputChannel: output,
	}
	filterEscalas := &FilterStopovers{
		filterId:  0,
		config:    &filters_config.FilterConfig{},
		consumer:  mockCons,
		producers: arrayProducers,
		filter:    filters.NewFilter(),
	}
	go filterEscalas.FilterStopovers()

	dynMap := make(map[string][]byte)
	dynMap["totalStopovers"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["totalStopovers"], uint32(2))
	row := data_structures.NewDynamicMap(dynMap)
	msgToSend := &data_structures.Message{TypeMessage: data_structures.FlightRows, DynMaps: []*data_structures.DynamicMap{row}}
	input <- msgToSend
	close(input)
	select {
	case <-output:
		t.Errorf("RowCount expected was 0")

	case <-time.After(2 * time.Second):
	}
}

func TestGettingARowWithTotalStopoversEqualToThreeShouldSendIt(t *testing.T) {
	input := make(chan *data_structures.Message)
	output := make(chan *data_structures.Message)

	mockCons := &mockConsumer{
		inputChannel: input,
		ok:           true,
	}
	arrayProducers := make([]queueProtocol.ProducerProtocolInterface, 1)
	arrayProducers[0] = &mockProducer{
		outputChannel: output,
	}
	filterEscalas := &FilterStopovers{
		filterId:  0,
		config:    &filters_config.FilterConfig{},
		consumer:  mockCons,
		producers: arrayProducers,
		filter:    filters.NewFilter(),
	}
	go filterEscalas.FilterStopovers()

	dynMap := make(map[string][]byte)
	dynMap["totalStopovers"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["totalStopovers"], uint32(3))
	row := data_structures.NewDynamicMap(dynMap)
	msgToSend := &data_structures.Message{TypeMessage: data_structures.FlightRows, DynMaps: []*data_structures.DynamicMap{row}}
	input <- msgToSend
	close(input)
	select {
	case result := <-output:
		newRow := result.DynMaps[0]
		ts, err := newRow.GetAsInt("totalStopovers")
		if err != nil {
			t.Errorf("Error getting totalStopovers...")
		}
		if ts < 3 {
			t.Errorf("Received a row that was not expected, has less than 3 stopovers...")
		}

	case <-time.After(1 * time.Second):
		t.Errorf("Timeout! Should have finished by now...")
	}
}

func TestGettingARowWithTotalStopoversGreaterThanThreeShouldSendIt(t *testing.T) {
	input := make(chan *data_structures.Message)
	output := make(chan *data_structures.Message)

	mockCons := &mockConsumer{
		inputChannel: input,
		ok:           true,
	}
	arrayProducers := make([]queueProtocol.ProducerProtocolInterface, 1)
	arrayProducers[0] = &mockProducer{
		outputChannel: output,
	}
	filterEscalas := &FilterStopovers{
		filterId:  0,
		config:    &filters_config.FilterConfig{},
		consumer:  mockCons,
		producers: arrayProducers,
		filter:    filters.NewFilter(),
	}
	go filterEscalas.FilterStopovers()

	dynMap := make(map[string][]byte)
	dynMap["totalStopovers"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["totalStopovers"], uint32(4))
	row := data_structures.NewDynamicMap(dynMap)
	msgToSend := &data_structures.Message{TypeMessage: data_structures.FlightRows, DynMaps: []*data_structures.DynamicMap{row}}
	input <- msgToSend
	close(input)
	select {
	case result := <-output:
		newRow := result.DynMaps[0]
		ts, err := newRow.GetAsInt("totalStopovers")
		if err != nil {
			t.Errorf("Error getting totalStopovers...")
		}
		if ts < 3 {
			t.Errorf("Received a row that was not expected, has less than 3 stopovers...")
		}
	case <-time.After(1 * time.Second):
		t.Errorf("Timeout! Should have finished by now...")
	}
}

func TestWithLessEqualAndGreaterCasesTogetherShouldSendTwoOutOfThree(t *testing.T) {
	input := make(chan *data_structures.Message, 3)
	output := make(chan *data_structures.Message, 3)

	mockCons := &mockConsumer{
		inputChannel: input,
		ok:           true,
	}
	arrayProducers := make([]queueProtocol.ProducerProtocolInterface, 1)
	arrayProducers[0] = &mockProducer{
		outputChannel: output,
	}
	filterEscalas := &FilterStopovers{
		filterId:  0,
		config:    &filters_config.FilterConfig{},
		consumer:  mockCons,
		producers: arrayProducers,
		filter:    filters.NewFilter(),
	}
	go filterEscalas.FilterStopovers()

	for i := 0; i < 3; i++ {
		dynMap := make(map[string][]byte)
		dynMap["totalStopovers"] = make([]byte, 4)
		binary.BigEndian.PutUint32(dynMap["totalStopovers"], uint32(2+i))
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
			ts, err := newRow.GetAsInt("totalStopovers")
			if err != nil {
				t.Errorf("Error getting totalStopovers...")
			}
			if ts < 3 {
				t.Errorf("Received a row that was not expected, has less than 3 stopovers...")
			}

		case <-time.After(1 * time.Second):
			// Should only read two messages from channel (3 and 4 stopovers)
			if rowCountRecvd != 2 {
				t.Errorf("Timeout! Should have finished by now...")
			}
		}
	}
	if rowCountRecvd != 2 {
		t.Errorf("Expected to receive only 2 rows, but %v were received", rowCountRecvd)
	}
}
