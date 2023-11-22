package duplicates

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOnArrivalOfNewClientIdItCreatesItsRegistry(t *testing.T) {
	duplicateDetector := NewDuplicatesHandler()
	assert.NotNil(t, duplicateDetector.lastMessagesSeen, "The map exists")

	duplicate := duplicateDetector.IsDuplicate(
		&dataStructures.Message{
			TypeMessage: dataStructures.FlightRows,
			ClientId:    "cliente nuevo",
			MessageId:   0,
			RowId:       5,
			DynMaps:     []*dataStructures.DynamicMap{},
		},
	)
	assert.Falsef(t, duplicate, "Should not be duplicate")
	client, exists := duplicateDetector.lastMessagesSeen["cliente nuevo"]
	assert.True(t, exists, "The client map exists")
	row, exists := client[0]
	assert.True(t, exists, "The message exists")
	assert.Equalf(t, uint16(5), row, "Expected and got row differ. Expected was: 5, got: %v", row)
}

func TestShouldDeleteTheShortestKeyWhenCheckingNewDuplicateAfterReachingMaxMessages(t *testing.T) {
	duplicateDetector := NewDuplicatesHandler()
	for i := uint(0); i < maxMessagesPerClient; i++ {
		duplicate := duplicateDetector.IsDuplicate(
			&dataStructures.Message{
				TypeMessage: dataStructures.FlightRows,
				ClientId:    "cliente nuevo",
				MessageId:   i,
				RowId:       uint16(i),
				DynMaps:     []*dataStructures.DynamicMap{},
			})
		assert.Falsef(t, duplicate, "Should not be duplicate")
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
			MessageId:   maxMessagesPerClient + 1,
			RowId:       uint16(maxMessagesPerClient + 1),
			DynMaps:     []*dataStructures.DynamicMap{},
		})
	assert.False(t, duplicate, "Expected the message not be duplicated")
	client, exists := duplicateDetector.lastMessagesSeen["cliente nuevo"]
	assert.True(t, exists, "The client map exists")
	_, exists = client[0]
	assert.False(t, exists, "The message should not exist")
	row, exists := client[maxMessagesPerClient+1]
	assert.True(t, exists, "The message should not exist")
	assert.Equalf(t, uint16(maxMessagesPerClient+1), row, "Expected and got row differ. Expected was: 5, got: %v", row)
}

func TestShouldReturnIsDuplicatedWhenTheClientMessageAndRowDoAlreadyExist(t *testing.T) {
	duplicateDetector := NewDuplicatesHandler()
	assert.NotNil(t, duplicateDetector.lastMessagesSeen, "The map exists")

	duplicate := duplicateDetector.IsDuplicate(
		&dataStructures.Message{
			TypeMessage: dataStructures.FlightRows,
			ClientId:    "cliente nuevo",
			MessageId:   0,
			RowId:       5,
			DynMaps:     []*dataStructures.DynamicMap{},
		},
	)
	assert.Falsef(t, duplicate, "Should not be duplicate")
	client, exists := duplicateDetector.lastMessagesSeen["cliente nuevo"]
	assert.True(t, exists, "The client map exists")
	row, exists := client[0]
	assert.True(t, exists, "The message exists")
	assert.Equalf(t, uint16(5), row, "Expected and got row differ. Expected was: 5, got: %v", row)

	// Check duplication and check if it does keep existing in the map
	duplicate = duplicateDetector.IsDuplicate(
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
