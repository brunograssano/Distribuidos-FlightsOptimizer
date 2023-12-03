package duplicates

import (
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
)

const maxMessagesPerClient = 200

type DuplicateDetector interface {
	checkpointer.Checkpointable
	IsDuplicate(message *dataStructures.Message) bool
	SaveMessageSeen(message *dataStructures.Message)
}

type DuplicatesHandler struct {
	//Structure ClientID -> MessageID -> RowID
	lastMessagesSeen map[string]map[uint]uint16
	queueName        string
}

func NewDuplicatesHandler(queueName string) *DuplicatesHandler {
	return &DuplicatesHandler{
		lastMessagesSeen: make(map[string]map[uint]uint16),
		queueName:        queueName,
	}
}

func (dh *DuplicatesHandler) getLengthAndShortestKey(clientId string) (int, int) {
	shortestKey := -1
	lenOfMessages := 0
	for key := range dh.lastMessagesSeen[clientId] {
		lenOfMessages++
		if int(key) < shortestKey || shortestKey == -1 {
			shortestKey = int(key)
		}
	}
	return lenOfMessages, shortestKey
}

func (dh *DuplicatesHandler) deleteOldMessagesFromMap(clientId string) {
	lenOfMessages, shortestKey := dh.getLengthAndShortestKey(clientId)
	if lenOfMessages == maxMessagesPerClient {
		delete(dh.lastMessagesSeen[clientId], uint(shortestKey))
	}
}

func (dh *DuplicatesHandler) IsDuplicate(message *dataStructures.Message) bool {
	isDuplicate := true
	_, exists := dh.lastMessagesSeen[message.ClientId]
	if !exists {
		dh.lastMessagesSeen[message.ClientId] = make(map[uint]uint16)
		isDuplicate = false
	}
	lenOfMessages, shortestKey := dh.getLengthAndShortestKey(message.ClientId)
	if lenOfMessages == maxMessagesPerClient && message.MessageId < uint(shortestKey) {
		return true
	}
	lastRowSeenFromMessage, exists := dh.lastMessagesSeen[message.ClientId][message.MessageId]
	if !exists || lastRowSeenFromMessage < message.RowId {
		isDuplicate = false
	}
	return isDuplicate
}

func (dh *DuplicatesHandler) SaveMessageSeen(message *dataStructures.Message) {
	_, exists := dh.lastMessagesSeen[message.ClientId]
	if !exists {
		dh.lastMessagesSeen[message.ClientId] = make(map[uint]uint16)
	}
	_, exists = dh.lastMessagesSeen[message.ClientId][message.MessageId]
	if !exists {
		dh.deleteOldMessagesFromMap(message.ClientId)
	}
	dh.lastMessagesSeen[message.ClientId][message.MessageId] = message.RowId
}
