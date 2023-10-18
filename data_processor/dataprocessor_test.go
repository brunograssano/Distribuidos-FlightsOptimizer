package main

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type (
	mockConsumer struct {
		inputChannel chan *dataStructures.Message
		ok           bool
	}
)

func (m *mockConsumer) GetReceivedMessages() int {
	return 0
}

func (m *mockConsumer) ClearData() {}

func (m *mockConsumer) Pop() (*dataStructures.Message, bool) {
	if !m.ok {
		return nil, m.ok
	}
	msg, ok := <-m.inputChannel
	return msg, ok
}

type (
	mockProducer struct {
		outputChannel chan *dataStructures.Message
	}
)

func (m *mockProducer) GetSentMessages() int {
	return 0
}

func (m *mockProducer) Send(data *dataStructures.Message) error {
	m.outputChannel <- data
	return nil
}

func (m *mockProducer) ClearData() {}

func TestShouldGetAMessageProcessItAndSendItToAllChannels(t *testing.T) {
	pConfig := &ProcessorConfig{}
	input := make(chan *dataStructures.Message, 10)
	outputEx13 := make(chan *dataStructures.Message, 10)
	outputEx2 := make(chan *dataStructures.Message, 10)
	outputEx4 := make(chan *dataStructures.Message, 10)

	mConsumer := &mockConsumer{
		inputChannel: input,
		ok:           true,
	}
	mProducer13 := &mockProducer{
		outputChannel: outputEx13,
	}
	mProducer2 := &mockProducer{
		outputChannel: outputEx2,
	}
	mProducer4 := &mockProducer{
		outputChannel: outputEx4,
	}

	processor := &DataProcessor{
		processorId:    0,
		c:              pConfig,
		consumer:       mConsumer,
		producersEx123: []protocol.ProducerProtocolInterface{mProducer2, mProducer13},
		producersEx4:   mProducer4,
		ex123Columns:   []string{utils.StartingAirport, utils.SegmentsArrivalAirportCode, utils.TotalStopovers, utils.Route},
		ex4Columns:     []string{utils.Route},
	}

	dynMap := make(map[string][]byte)
	dynMap[utils.StartingAirport] = []byte("FRA")
	dynMap[utils.SegmentsArrivalAirportCode] = []byte("EZE")
	dynMap["col"] = []byte("Even more data")

	row := dataStructures.NewDynamicMap(dynMap)

	go processor.ProcessData()
	rows := []*dataStructures.DynamicMap{row}
	input <- &dataStructures.Message{TypeMessage: dataStructures.FlightRows, DynMaps: rows}
	close(input)

	sentResponseToAll := [3]bool{false, false, false}
	for i := 0; i < 3; i++ {
		select {
		case result := <-outputEx13:
			newRow := result.DynMaps[0]
			assert.Equalf(t, uint32(4), newRow.GetColumnCount(), "RowCount expected was 4, got %v", newRow.GetColumnCount())
			sentResponseToAll[0] = true
		case result := <-outputEx2:
			newRow := result.DynMaps[0]
			assert.Equalf(t, uint32(4), newRow.GetColumnCount(), "RowCount expected was 4, got %v", newRow.GetColumnCount())
			sentResponseToAll[1] = true
		case result := <-outputEx4:
			newRow := result.DynMaps[0]
			assert.Equalf(t, uint32(1), newRow.GetColumnCount(), "RowCount expected was 1, got %v", newRow.GetColumnCount())
			sentResponseToAll[2] = true
		case <-time.After(1 * time.Second):
			t.Errorf("Timeout! Should have finished by now...")

		}
	}
	for i := 0; i < 3; i++ {
		assert.True(t, sentResponseToAll[i], "Missing response from a channel")
	}
}

func TestShouldProcessTheDataOfEx123(t *testing.T) {
	processor := &DataProcessor{
		processorId:  0,
		ex123Columns: []string{utils.TotalStopovers, utils.Route},
	}
	dynMap := make(map[string][]byte)
	dynMap[utils.StartingAirport] = []byte("FRA")
	dynMap[utils.SegmentsArrivalAirportCode] = []byte("CDG||EZE")
	dynMap["col"] = []byte("Even more data")

	row := dataStructures.NewDynamicMap(dynMap)
	row, err := processor.processEx123Row(row)

	assert.Nilf(t, err, "Got error when processing ex123 row: %v", err)

	route, err := row.GetAsString(utils.Route)
	assert.Nilf(t, err, "Got error when getting route: %v", err)
	assert.Equalf(t, "FRA||CDG||EZE", route, "Expecting FRA||CDG||EZE route but got %v", route)

	stopovers, err := row.GetAsInt(utils.TotalStopovers)
	assert.Nilf(t, err, "Got error when getting stopovers: %v", err)
	assert.Equalf(t, 1, stopovers, "Expecting 1 stopover but got %v", stopovers)
}

func TestShouldReturnAnErrorIfTheSegmentsColDoesNotExist(t *testing.T) {
	processor := &DataProcessor{
		processorId:  0,
		ex123Columns: []string{utils.TotalStopovers, utils.Route},
	}
	dynMap := make(map[string][]byte)
	dynMap[utils.StartingAirport] = []byte("FRA")

	row := dataStructures.NewDynamicMap(dynMap)
	row, err := processor.processEx123Row(row)

	assert.Error(t, err, "Didn't got an error when processing segments column")
}

func TestShouldReturnAnErrorIfTheStartingAirportColDoesNotExist(t *testing.T) {
	processor := &DataProcessor{
		processorId:  0,
		ex123Columns: []string{utils.TotalStopovers, utils.Route},
	}
	dynMap := make(map[string][]byte)
	dynMap[utils.SegmentsArrivalAirportCode] = []byte("CDG||EZE")

	row := dataStructures.NewDynamicMap(dynMap)
	row, err := processor.processEx123Row(row)

	assert.Error(t, err, "Didn't got an error when processing starting column")
}
