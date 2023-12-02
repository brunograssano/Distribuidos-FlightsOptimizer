package duplicates

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOnArrivalOfNewClientIdItCreatesItsRegistry(t *testing.T) {
	duplicateDetector := NewDuplicatesHandler("cola")
	assert.NotNil(t, duplicateDetector.lastMessagesSeen, "The map exists")

	duplicateDetector.SaveMessageSeen(
		&dataStructures.Message{
			TypeMessage: dataStructures.FlightRows,
			ClientId:    "cliente nuevo",
			MessageId:   0,
			RowId:       5,
			DynMaps:     []*dataStructures.DynamicMap{},
		},
	)
	client, exists := duplicateDetector.lastMessagesSeen["cliente nuevo"]
	assert.True(t, exists, "The client map exists")
	row, exists := client[0]
	assert.True(t, exists, "The message exists")
	assert.Equalf(t, uint16(5), row, "Expected and got row differ. Expected was: 5, got: %v", row)
}

func TestShouldDeleteTheShortestKeyWhenCheckingNewDuplicateAfterReachingMaxMessages(t *testing.T) {
	duplicateDetector := NewDuplicatesHandler("cola")
	for i := uint(0); i < maxMessagesPerClient; i++ {
		duplicateDetector.SaveMessageSeen(
			&dataStructures.Message{
				TypeMessage: dataStructures.FlightRows,
				ClientId:    "cliente nuevo",
				MessageId:   i,
				RowId:       uint16(i),
				DynMaps:     []*dataStructures.DynamicMap{},
			},
		)
		client, exists := duplicateDetector.lastMessagesSeen["cliente nuevo"]
		assert.True(t, exists, "The client map exists")
		row, exists := client[i]
		assert.True(t, exists, "The message exists")
		assert.Equalf(t, uint16(i), row, "Expected and got row differ. Expected was: 5, got: %v", row)
	}

	newMsg := &dataStructures.Message{
		TypeMessage: dataStructures.FlightRows,
		ClientId:    "cliente nuevo",
		MessageId:   maxMessagesPerClient + 1,
		RowId:       uint16(maxMessagesPerClient + 1),
		DynMaps:     []*dataStructures.DynamicMap{},
	}
	duplicate := duplicateDetector.IsDuplicate(newMsg)
	assert.False(t, duplicate, "Expected the message not be duplicated")
	duplicateDetector.SaveMessageSeen(newMsg)
	client, exists := duplicateDetector.lastMessagesSeen["cliente nuevo"]
	assert.True(t, exists, "The client map exists")
	_, exists = client[0]
	assert.False(t, exists, "The message should not exist")
	row, exists := client[maxMessagesPerClient+1]
	assert.True(t, exists, "The message should exist")
	assert.Equalf(t, uint16(maxMessagesPerClient+1), row, "Expected and got row differ. Expected was: 5, got: %v", row)
}

func TestShouldReturnIsDuplicatedWhenTheClientMessageAndRowDoAlreadyExist(t *testing.T) {
	duplicateDetector := NewDuplicatesHandler("cola")
	assert.NotNil(t, duplicateDetector.lastMessagesSeen, "The map exists")

	duplicateDetector.SaveMessageSeen(
		&dataStructures.Message{
			TypeMessage: dataStructures.FlightRows,
			ClientId:    "cliente nuevo",
			MessageId:   0,
			RowId:       5,
			DynMaps:     []*dataStructures.DynamicMap{},
		},
	)
	client, exists := duplicateDetector.lastMessagesSeen["cliente nuevo"]
	assert.True(t, exists, "The client map exists")
	row, exists := client[0]
	assert.True(t, exists, "The message exists")
	assert.Equalf(t, uint16(5), row, "Expected and got row differ. Expected was: 5, got: %v", row)

	// Check duplication and check if it does keep existing in the map
	duplicate := duplicateDetector.IsDuplicate(
		&dataStructures.Message{
			TypeMessage: dataStructures.FlightRows,
			ClientId:    "cliente nuevo",
			MessageId:   0,
			RowId:       5,
			DynMaps:     []*dataStructures.DynamicMap{},
		},
	)
	assert.Truef(t, duplicate, "Should be duplicate")
	client, exists = duplicateDetector.lastMessagesSeen["cliente nuevo"]
	assert.True(t, exists, "The client map exists")
	row, exists = client[0]
	assert.True(t, exists, "The message exists")
	assert.Equalf(t, uint16(5), row, "Expected and got row differ. Expected was: 5, got: %v", row)
}

func TestShouldThrowDuplicateWhenMessageIdIsLessThanLeastKeyAndMapIsFull(t *testing.T) {
	duplicateDetector := NewDuplicatesHandler("cola")
	for i := uint(100); i < 100+maxMessagesPerClient; i++ {
		duplicateDetector.SaveMessageSeen(
			&dataStructures.Message{
				TypeMessage: dataStructures.FlightRows,
				ClientId:    "cliente nuevo",
				MessageId:   i,
				RowId:       uint16(i),
				DynMaps:     []*dataStructures.DynamicMap{},
			})
		client, exists := duplicateDetector.lastMessagesSeen["cliente nuevo"]
		assert.True(t, exists, "The client map exists")
		row, exists := client[i]
		assert.True(t, exists, "The message exists")
		assert.Equalf(t, uint16(i), row, "Expected and got row differ. Expected was: 5, got: %v", row)
	}

	duplicate := duplicateDetector.IsDuplicate(
		&dataStructures.Message{
			TypeMessage: dataStructures.FlightRows,
			ClientId:    "cliente nuevo",
			MessageId:   99,
			RowId:       uint16(99),
			DynMaps:     []*dataStructures.DynamicMap{},
		})
	assert.True(t, duplicate, "Expected the message to be duplicated")
	client, exists := duplicateDetector.lastMessagesSeen["cliente nuevo"]
	assert.True(t, exists, "The client map exists")
	_, exists = client[99]
	assert.False(t, exists, "The message should not exist")
	row, exists := client[100]
	assert.True(t, exists, "The message should exist")
	assert.Equalf(t, uint16(100), row, "Expected and got row differ. Expected was: 5, got: %v", row)
}
