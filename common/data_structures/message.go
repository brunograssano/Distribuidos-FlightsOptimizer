package data_structures

type Message struct {
	TypeMessage int
	ClientId    string
	MessageId   uint
	RowId       uint16
	DynMaps     []*DynamicMap
}

func NewMessageWithoutData(oldMessage *Message) *Message {
	return &Message{
		TypeMessage: oldMessage.TypeMessage,
		ClientId:    oldMessage.ClientId,
		MessageId:   oldMessage.MessageId,
		RowId:       oldMessage.RowId,
		DynMaps:     make([]*DynamicMap, 0),
	}
}

func NewMessageWithData(oldMessage *Message, data []*DynamicMap) *Message {
	return &Message{
		TypeMessage: oldMessage.TypeMessage,
		ClientId:    oldMessage.ClientId,
		MessageId:   oldMessage.MessageId,
		RowId:       oldMessage.RowId,
		DynMaps:     data,
	}
}

func NewMessageWithDataAndRowId(oldMessage *Message, data []*DynamicMap, rowId uint16) *Message {
	return &Message{
		TypeMessage: oldMessage.TypeMessage,
		ClientId:    oldMessage.ClientId,
		MessageId:   oldMessage.MessageId,
		RowId:       rowId,
		DynMaps:     data,
	}
}

func NewTypeMessageWithoutData(messageType int, oldMessage *Message) *Message {
	return &Message{
		TypeMessage: messageType,
		ClientId:    oldMessage.ClientId,
		MessageId:   oldMessage.MessageId,
		RowId:       oldMessage.RowId,
		DynMaps:     make([]*DynamicMap, 0),
	}
}

func NewTypeMessageWithoutDataAndMsgId(messageType int, oldMessage *Message, msgId uint) *Message {
	return &Message{
		TypeMessage: messageType,
		ClientId:    oldMessage.ClientId,
		MessageId:   msgId,
		RowId:       oldMessage.RowId,
		DynMaps:     make([]*DynamicMap, 0),
	}
}

func NewTypeMessageWithData(messageType int, oldMessage *Message, data []*DynamicMap) *Message {
	return &Message{
		TypeMessage: messageType,
		ClientId:    oldMessage.ClientId,
		MessageId:   oldMessage.MessageId,
		RowId:       oldMessage.RowId,
		DynMaps:     data,
	}
}

func NewTypeMessageWithDataAndMsgId(messageType int, oldMessage *Message, data []*DynamicMap, msgId uint) *Message {
	return &Message{
		TypeMessage: messageType,
		ClientId:    oldMessage.ClientId,
		MessageId:   msgId,
		RowId:       oldMessage.RowId,
		DynMaps:     data,
	}
}

func NewTypeMessageWithDataRowIdAndMsgId(messageType int, oldMessage *Message, data []*DynamicMap, rowId uint16, msgId uint) *Message {
	return &Message{
		TypeMessage: messageType,
		ClientId:    oldMessage.ClientId,
		MessageId:   msgId,
		RowId:       rowId,
		DynMaps:     data,
	}
}

func NewGetResultsMessage(clientId string) *Message {
	return &Message{TypeMessage: GetResults, ClientId: clientId}
}

func NewCompleteMessage(messageType int, data []*DynamicMap, clientId string, msgId uint) *Message {
	return &Message{
		TypeMessage: messageType,
		ClientId:    clientId,
		MessageId:   msgId,
		RowId:       0,
		DynMaps:     data,
	}
}

const Airports = 0
const EOFAirports = 1
const FlightRows = 2
const EOFFlightRows = 3
const GetResults = 4
const Later = 5
const EOFGetter = 6
const FinalAvgMsg = 7
const HeartBeat = 8
