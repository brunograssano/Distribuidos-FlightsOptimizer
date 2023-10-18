package getters

import (
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	"testing"
	"time"
)

func TestShouldReturnLaterMessage(t *testing.T) {
	resultChan := make(chan bool, 1)
	config := &GetterConfig{Address: "127.0.0.1:45678", MaxLinesPerSend: 1}
	canSend := make(chan string, 1)
	getter, err := NewGetter(config, canSend)
	if err != nil {
		t.Errorf("Error creating getter: %v", err)
	}

	go getter.ReturnResults()

	conn, err := communication.NewActiveTCPSocket("127.0.0.1:45678")
	if err != nil {
		t.Errorf("Error connecting to getter: %v", err)
	}
	socketProtocol := protocol.NewSocketProtocolHandler(conn)

	go func() {
		msg, err := socketProtocol.Read()
		if err != nil {
			t.Errorf("Error receiving msg: %v", err)
		}
		if msg.TypeMessage != dataStructures.Later {
			t.Errorf("Msg is not later: %v", msg.TypeMessage)
		}
		resultChan <- true
	}()
	select {
	case <-resultChan:
	case <-time.After(1 * time.Second):
		t.Errorf("Timeout! Should have finished by now...")
	}
}
