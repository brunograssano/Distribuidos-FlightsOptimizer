package reducer

import (
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type (
	mockConsumerQueueProtocolHandler struct {
		inputChannel chan *dataStructures.Message
		ok           bool
	}
)

func (m *mockConsumerQueueProtocolHandler) DoCheckpoint(errors chan error, i int, i2 int) {
	//TODO implement me
	panic("implement me")
}

func (m *mockConsumerQueueProtocolHandler) RestoreCheckpoint(i int, i2 int, errors chan error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockConsumerQueueProtocolHandler) GetCheckpointVersions(i int) [2]int {
	//TODO implement me
	panic("implement me")
}

func (m *mockConsumerQueueProtocolHandler) Commit(i int, errors chan error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockConsumerQueueProtocolHandler) Abort(i int, errors chan error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockConsumerQueueProtocolHandler) SetStatusOfLastMessage(b bool) {}

func (m *mockConsumerQueueProtocolHandler) ClearData(s string) {}

func (m *mockConsumerQueueProtocolHandler) GetReceivedMessages(s string) int {
	return 0
}

func (m *mockConsumerQueueProtocolHandler) Pop() (*dataStructures.Message, bool) {
	if !m.ok {
		return nil, m.ok
	}
	msg, ok := <-m.inputChannel
	return msg, ok
}

func (m *mockConsumerQueueProtocolHandler) BindTo(_ string, _ string, _ string) error {
	return nil
}

type (
	mockProducerQueueProtocolHandler struct {
		outputChannel chan *dataStructures.Message
	}
)

func (m *mockProducerQueueProtocolHandler) ClearData(s string) {}

func (m *mockProducerQueueProtocolHandler) Send(msg *dataStructures.Message) error {
	m.outputChannel <- msg
	return nil
}

func (m *mockProducerQueueProtocolHandler) GetSentMessages(s string) int {
	return 0
}

func TestShouldGetAMessageReduceItAndSendIt(t *testing.T) {
	reducerConfig := &Config{ColumnsToKeep: []string{"col1"}}
	input := make(chan *dataStructures.Message, 10)
	output := make(chan *dataStructures.Message, 10)

	mConsumer := &mockConsumerQueueProtocolHandler{
		inputChannel: input,
		ok:           true,
	}
	mProducer := &mockProducerQueueProtocolHandler{
		outputChannel: output,
	}

	reducer := &Reducer{
		reducerId:    0,
		c:            reducerConfig,
		consumer:     mConsumer,
		producer:     mProducer,
		checkpointer: checkpointer.NewCheckpointerHandler(),
	}

	dynMap := make(map[string][]byte)
	dynMap["col1"] = []byte("Some data")
	dynMap["col2"] = []byte("More data")

	row := dataStructures.NewDynamicMap(dynMap)

	go reducer.ReduceDims()
	rows := []*dataStructures.DynamicMap{row}
	input <- &dataStructures.Message{TypeMessage: dataStructures.FlightRows, DynMaps: rows}
	close(input)

	select {
	case result := <-output:
		newRow := result.DynMaps[0]
		assert.Equalf(t, uint32(1), newRow.GetColumnCount(), "RowCount expected was 1, got %v", newRow.GetColumnCount())

	case <-time.After(1 * time.Second):
		t.Errorf("Timeout! Should have finished by now...")

	}

}
