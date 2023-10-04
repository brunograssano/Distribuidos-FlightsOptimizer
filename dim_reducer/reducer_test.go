package main

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"testing"
	"time"
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

func TestShouldGetAMessageReduceItAndSendIt(t *testing.T) {
	reducerConfig := &ReducerConfig{ColumnsToKeep: []string{"col1"}}
	input := make(chan []byte, 10)
	output := make(chan []byte, 10)
	serializer := dataStructures.NewSerializer()

	mConsumer := &mockConsumer{
		inputChannel: input,
		ok:           true,
	}
	mProducer := &mockProducer{
		outputChannel: output,
	}

	reducer := &Reducer{
		reducerId:  0,
		c:          reducerConfig,
		consumer:   mConsumer,
		producer:   mProducer,
		serializer: serializer,
	}

	dynMap := make(map[string][]byte)
	dynMap["col1"] = []byte("Some data")
	dynMap["col2"] = []byte("More data")

	row := dataStructures.NewDynamicMap(dynMap)

	go reducer.ReduceDims()
	rows := []*dataStructures.DynamicMap{row}
	input <- serializer.SerializeMsg(&dataStructures.Message{TypeMessage: dataStructures.FlightRows, DynMaps: rows})
	close(input)

	select {
	case result := <-output:
		newRow := serializer.DeserializeMsg(result).DynMaps[0]
		if newRow.GetColumnCount() != 1 {
			t.Errorf("RowCount expected was 1")
		}

	case <-time.After(1 * time.Second):
		t.Errorf("Timeout! Should have finished by now...")

	}

}
