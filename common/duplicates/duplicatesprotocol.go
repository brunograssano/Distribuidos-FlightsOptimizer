package duplicates

import dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"

const maxMessagesPerClient = 200

type DuplicateDetector interface {
	IsDuplicate(message *dataStructures.Message) bool
}

type DuplicatesHandler struct {
	//Structure ClientID -> MessageID -> RowID
	lastMessagesSeen map[string]map[uint]uint16
}

func NewDuplicatesHandler() *DuplicatesHandler {
	return &DuplicatesHandler{lastMessagesSeen: make(map[string]map[uint]uint16)}
}

func (dh *DuplicatesHandler) deleteOldMessagesFromMap(clientId string) {
	lenOfMessages := 0
	shortestKey := -1
	for key := range dh.lastMessagesSeen[clientId] {
		lenOfMessages++
		if int(key) < shortestKey || shortestKey == -1 {
			shortestKey = int(key)
		}
	}
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
	lastRowSeenFromMessage, exists := dh.lastMessagesSeen[message.ClientId][message.MessageId]
	if !exists || lastRowSeenFromMessage < message.RowId {
		if !exists {
			dh.deleteOldMessagesFromMap(message.ClientId)
		}
		dh.lastMessagesSeen[message.ClientId][message.MessageId] = message.RowId
		isDuplicate = false
	}
	return isDuplicate
}
