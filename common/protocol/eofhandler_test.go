package protocol

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	"testing"
	"time"
)

type (
	mockConsumerQueueProtocolHandler struct {
		inputChannel chan *dataStructures.Message
		ok           bool
		count        int
	}
)

func (m *mockConsumerQueueProtocolHandler) GetReceivedMessages() int {
	return m.count
}

func (m *mockConsumerQueueProtocolHandler) Pop() (*dataStructures.Message, bool) {
	if !m.ok {
		return nil, m.ok
	}
	msg, ok := <-m.inputChannel
	return msg, ok
}

func (m *mockConsumerQueueProtocolHandler) BindTo(_ string, _ string) error {
	return nil
}

func (m *mockConsumerQueueProtocolHandler) ClearData() {
	m.count = 0
}

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

func (m *mockProducerQueueProtocolHandler) GetSentMessages() int {
	return m.count
}

func (m *mockProducerQueueProtocolHandler) ClearData() {
	m.count = 0
}

func TestShouldSendEOFToTheNextStepOnGreaterThanPrevSent(t *testing.T) {
	outNext := make(chan *dataStructures.Message, 1)
	outSame := make(chan *dataStructures.Message, 1)
	dynMap := make(map[string][]byte)
	dynMap[utils.PrevSent] = serializer.SerializeUint(4)
	localSent := 2
	dynMap[utils.LocalSent] = serializer.SerializeUint(uint32(localSent))
	dynMap[utils.LocalReceived] = serializer.SerializeUint(3)
	msg := &dataStructures.Message{
		TypeMessage: dataStructures.EOFFlightRows,
		DynMaps:     []*dataStructures.DynamicMap{dataStructures.NewDynamicMap(dynMap)},
	}

	consumer := &mockConsumerQueueProtocolHandler{count: 10}
	nextStep := &mockProducerQueueProtocolHandler{outputChannel: outNext, count: 5}
	sameStep := &mockProducerQueueProtocolHandler{outputChannel: outSame, count: 5}
	go func() {
		err := HandleEOF(msg, consumer, sameStep, []ProducerProtocolInterface{nextStep})
		if err != nil {
			t.Errorf("Should not have thrown error handling EOF.")
		}
	}()
	select {
	case messageReceivedInNextStep := <-outNext:

		prevSent, err := messageReceivedInNextStep.DynMaps[0].GetAsInt(utils.PrevSent)
		if err != nil {
			t.Errorf("Should not have thrown error getting prevSent in next step.")
		}
		realPrevSent := 5 + localSent
		if prevSent != realPrevSent {
			t.Errorf("Expected to get %v as prevSent, but got %v", realPrevSent, prevSent)
		}

		localSentChan, err := messageReceivedInNextStep.DynMaps[0].GetAsInt(utils.LocalSent)
		if err != nil {
			t.Errorf("Should not have thrown error getting localSent in next step.")
		}
		if localSentChan != 0 {
			t.Errorf("Expected localSent to be %v. It was: %v", 0, localSentChan)
		}
		localReceivedChan, err := messageReceivedInNextStep.DynMaps[0].GetAsInt(utils.LocalReceived)
		if err != nil {
			t.Errorf("Should not have thrown error getting localReceived in next step.")
		}
		if localReceivedChan != 0 {
			t.Errorf("Expected to get %v as localReceivedChan, but got %v", 0, localReceivedChan)
		}

	case <-time.After(1 * time.Second):
		t.Errorf("Timeout! Should have finished by now...")
	}
}

func TestShouldSendEOFToTheNextStepOnEqualToPrevSent(t *testing.T) {
	outNext := make(chan *dataStructures.Message)
	outSame := make(chan *dataStructures.Message)
	dynMap := make(map[string][]byte)
	dynMap["prevSent"] = serializer.SerializeUint(13)
	localSent := 2
	dynMap["localSent"] = serializer.SerializeUint(uint32(localSent))
	dynMap["localReceived"] = serializer.SerializeUint(3)
	msg := &dataStructures.Message{
		TypeMessage: dataStructures.EOFFlightRows,
		DynMaps:     []*dataStructures.DynamicMap{dataStructures.NewDynamicMap(dynMap)},
	}

	consumer := &mockConsumerQueueProtocolHandler{count: 10}
	nextStep := &mockProducerQueueProtocolHandler{outputChannel: outNext, count: 5}
	sameStep := &mockProducerQueueProtocolHandler{outputChannel: outSame, count: 5}
	go func() {
		err := HandleEOF(msg, consumer, sameStep, []ProducerProtocolInterface{nextStep})
		if err != nil {
			t.Errorf("Should not have thrown error handling EOF.")
		}
	}()
	select {
	case messageReceivedInNextStep := <-outNext:

		prevSent, err := messageReceivedInNextStep.DynMaps[0].GetAsInt(utils.PrevSent)
		if err != nil {
			t.Errorf("Should not have thrown error getting prevSent in next step.")
		}
		realPrevSent := 5 + localSent
		if prevSent != realPrevSent {
			t.Errorf("Expected to get %v as prevSent, but got %v", realPrevSent, prevSent)
		}

		localSentChan, err := messageReceivedInNextStep.DynMaps[0].GetAsInt(utils.LocalSent)
		if err != nil {
			t.Errorf("Should not have thrown error getting localSent in next step.")
		}
		if localSentChan != 0 {
			t.Errorf("Expected localSent to be %v. It was: %v", 0, localSentChan)
		}
		localReceivedChan, err := messageReceivedInNextStep.DynMaps[0].GetAsInt(utils.LocalReceived)
		if err != nil {
			t.Errorf("Should not have thrown error getting localReceived in next step.")
		}
		if localReceivedChan != 0 {
			t.Errorf("Expected to get %v as localReceivedChan, but got %v", 0, localReceivedChan)
		}

	case <-time.After(1 * time.Second):
		t.Errorf("Timeout! Should have finished by now...")
	}
}

func TestShouldSendEOFToTheSameStepOnLessThanPrevSent(t *testing.T) {
	outNext := make(chan *dataStructures.Message)
	outSame := make(chan *dataStructures.Message)
	dynMap := make(map[string][]byte)
	dynMap["prevSent"] = serializer.SerializeUint(204)
	localSent := 2
	dynMap["localSent"] = serializer.SerializeUint(uint32(localSent))
	dynMap["localReceived"] = serializer.SerializeUint(3)
	msg := &dataStructures.Message{
		TypeMessage: dataStructures.EOFFlightRows,
		DynMaps:     []*dataStructures.DynamicMap{dataStructures.NewDynamicMap(dynMap)},
	}

	consumer := &mockConsumerQueueProtocolHandler{count: 10}
	nextStep := &mockProducerQueueProtocolHandler{outputChannel: outNext, count: 5}
	sameStep := &mockProducerQueueProtocolHandler{outputChannel: outSame, count: 5}
	go func() {
		err := HandleEOF(msg, consumer, sameStep, []ProducerProtocolInterface{nextStep})
		if err != nil {
			t.Errorf("Should not have thrown error handling EOF.")
		}
	}()
	select {
	case messageReceivedInNextStep := <-outSame:

		localSentReceived, err := messageReceivedInNextStep.DynMaps[0].GetAsInt(utils.LocalSent)
		if err != nil {
			t.Errorf("Should not have thrown error getting localSent in same step.")
		}
		realLocalSent := 5 + localSent
		if realLocalSent != localSentReceived {
			t.Errorf("Expected to get %v as prevSent, but got %v", realLocalSent, localSentReceived)
		}

		prevSentReceived, err := messageReceivedInNextStep.DynMaps[0].GetAsInt(utils.PrevSent)
		if err != nil {
			t.Errorf("Should not have thrown error getting prevSent in same step.")
		}
		if prevSentReceived != 204 {
			t.Errorf("prevSentReceived should be 204 but got %v", prevSentReceived)
		}

		localReceivedReceived, err := messageReceivedInNextStep.DynMaps[0].GetAsInt(utils.LocalReceived)
		realLocalReceived := 10 + 3
		if err != nil {
			t.Errorf("Should not have thrown error getting localReceived in same step.")
		}
		if realLocalReceived != localReceivedReceived {
			t.Errorf("Expected to get %v as localReceived, but got %v", realLocalReceived, localReceivedReceived)
		}

	case <-time.After(1 * time.Second):
		t.Errorf("Timeout! Should have finished by now...")
	}
}
