package queues

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type (
	mockProducerQueueProtocolHandler struct {
		outputChannel chan *dataStructures.Message
		count         int
	}
)

func (m *mockProducerQueueProtocolHandler) Send(msg *dataStructures.Message) error {
	m.outputChannel <- msg
	return nil
}

func TestShouldSendEOFToTheSameStepIfDoesNotMeetTheTotalNodes(t *testing.T) {
	outNext := make(chan *dataStructures.Message, 1)
	outSame := make(chan *dataStructures.Message, 1)
	totalNodes := 3
	dynMap := make(map[string][]byte)
	dynMap[utils.NodesVisited] = serializer.SerializeString("")
	msg := &dataStructures.Message{
		TypeMessage: dataStructures.EOFFlightRows,
		DynMaps:     []*dataStructures.DynamicMap{dataStructures.NewDynamicMap(dynMap)},
	}

	nextStep := &mockProducerQueueProtocolHandler{outputChannel: outNext}
	sameStep := &mockProducerQueueProtocolHandler{outputChannel: outSame}
	go func() {
		err := HandleEOF(msg, sameStep, []ProducerProtocolInterface{nextStep}, "id_prueba", uint(totalNodes))
		assert.Nil(t, err, "Should not have thrown error handling EOF.")
	}()
	select {
	case messageReceivedInNextStep := <-outSame:

		nodesVisited, err := messageReceivedInNextStep.DynMaps[0].GetAsString(utils.NodesVisited)
		assert.Nil(t, err, "Should not have thrown error getting nodesVisited in next step.")
		assert.Equalf(t, "id_prueba", nodesVisited, "Nodes visited does not match expected. expected: id_prueba, got: %v", nodesVisited)

	case <-time.After(1 * time.Second):
		t.Errorf("Timeout! Should have finished by now...")
	}
}

func TestShouldSendEOFToTheNextStepIfItVisitedAll(t *testing.T) {
	outNext := make(chan *dataStructures.Message)
	outSame := make(chan *dataStructures.Message)
	dynMap := make(map[string][]byte)
	dynMap[utils.NodesVisited] = serializer.SerializeString("1,2,3")
	msg := &dataStructures.Message{
		TypeMessage: dataStructures.EOFFlightRows,
		DynMaps:     []*dataStructures.DynamicMap{dataStructures.NewDynamicMap(dynMap)},
	}

	nextStep := &mockProducerQueueProtocolHandler{outputChannel: outNext}
	sameStep := &mockProducerQueueProtocolHandler{outputChannel: outSame}
	go func() {
		err := HandleEOF(msg, sameStep, []ProducerProtocolInterface{nextStep}, "4", 4)
		assert.Nil(t, err, "Should not have thrown error handling EOF.")
	}()
	select {
	case messageReceivedInNextStep := <-outNext:

		visited, err := messageReceivedInNextStep.DynMaps[0].GetAsString(utils.NodesVisited)
		assert.Nil(t, err, "Should not have thrown error getting nodesVisited in next step.")
		assert.Equalf(t, "", visited, "Visited should be empty")

	case <-time.After(1 * time.Second):
		t.Errorf("Timeout! Should have finished by now...")
	}
}

func TestShouldSendEOFToTheSameStepWithoutAddingNodesIfItIsAlreadyInList(t *testing.T) {
	outNext := make(chan *dataStructures.Message, 1)
	outSame := make(chan *dataStructures.Message, 1)
	totalNodes := 3
	dynMap := make(map[string][]byte)
	dynMap[utils.NodesVisited] = serializer.SerializeString("id_prueba")
	msg := &dataStructures.Message{
		TypeMessage: dataStructures.EOFFlightRows,
		DynMaps:     []*dataStructures.DynamicMap{dataStructures.NewDynamicMap(dynMap)},
	}

	nextStep := &mockProducerQueueProtocolHandler{outputChannel: outNext}
	sameStep := &mockProducerQueueProtocolHandler{outputChannel: outSame}
	go func() {
		err := HandleEOF(msg, sameStep, []ProducerProtocolInterface{nextStep}, "id_prueba", uint(totalNodes))
		assert.Nil(t, err, "Should not have thrown error handling EOF.")
	}()
	select {
	case messageReceivedInNextStep := <-outSame:

		nodesVisited, err := messageReceivedInNextStep.DynMaps[0].GetAsString(utils.NodesVisited)
		assert.Nil(t, err, "Should not have thrown error getting nodesVisited in next step.")
		assert.Equalf(t, "id_prueba", nodesVisited, "Nodes visited does not match expected. expected: id_prueba, got: %v", nodesVisited)

	case <-time.After(1 * time.Second):
		t.Errorf("Timeout! Should have finished by now...")
	}
}
