package data_structures

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
)

const commaSeparator = ","
const newLine = "\n"

type Serializer struct{}

func NewSerializer() *Serializer {
	return &Serializer{}
}

func (serializer *Serializer) SerializeMsg(msg *Message) []byte {
	serializedMsg := []byte{}
	typeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(typeBytes, uint32(msg.TypeMessage))
	nRows := make([]byte, 4)
	binary.BigEndian.PutUint32(nRows, uint32(len(msg.DynMaps)))
	serializedMsg = append(serializedMsg, typeBytes...)
	serializedMsg = append(serializedMsg, nRows...)
	for _, row := range msg.DynMaps {
		serializedRow := serializer.SerializeDynMap(row)
		serializedMsg = append(serializedMsg, serializedRow...)
	}
	return serializedMsg
}

func (serializer *Serializer) DeserializeMsg(bytesMsg []byte) *Message {
	offset := 0
	typeMsg := int(binary.BigEndian.Uint32(bytesMsg[offset : offset+4]))
	offset = 4
	nRows := int(binary.BigEndian.Uint32(bytesMsg[offset : offset+4]))
	offset = 8
	var dynMaps []*DynamicMap
	for i := 0; i < nRows; i++ {
		dynMap, bytesRead := serializer.DeserializeDynMap(bytesMsg[offset:])
		dynMaps = append(dynMaps, dynMap)
		offset += bytesRead
	}
	return &Message{
		TypeMessage: typeMsg,
		DynMaps:     dynMaps,
	}
}

func (serializer *Serializer) SerializeDynMap(dynamicMap *DynamicMap) []byte {
	var rowBytes []byte
	mapLength := dynamicMap.GetColumnCount()
	bytesNCols := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesNCols, mapLength)
	rowBytes = append(rowBytes, bytesNCols...)
	currMap := dynamicMap.GetCurrentMap()
	for key, value := range currMap {
		keyLengthBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(keyLengthBytes, uint32(len(key)))
		keyBytes := []byte(key)
		valueLengthBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(valueLengthBytes, uint32(len(value)))
		rowBytes = append(rowBytes, keyLengthBytes...)
		rowBytes = append(rowBytes, keyBytes...)
		rowBytes = append(rowBytes, valueLengthBytes...)
		rowBytes = append(rowBytes, value...)
	}
	return rowBytes
}

func (serializer *Serializer) DeserializeDynMap(dynamicMapBytes []byte) (*DynamicMap, int) {
	nCols := int(binary.BigEndian.Uint32(dynamicMapBytes[0:4]))
	currOffset := 4
	mapForDynMap := make(map[string][]byte)
	for i := 0; i < nCols; i++ {
		lenKey := int(binary.BigEndian.Uint32(dynamicMapBytes[currOffset : currOffset+4]))
		currOffset += 4
		key := string(dynamicMapBytes[currOffset : currOffset+lenKey])
		currOffset += lenKey
		lenValue := int(binary.BigEndian.Uint32(dynamicMapBytes[currOffset : currOffset+4]))
		currOffset += 4
		value := dynamicMapBytes[currOffset : currOffset+lenValue]
		currOffset += lenValue
		mapForDynMap[key] = value
	}
	return NewDynamicMap(mapForDynMap), currOffset
}

func (serializer *Serializer) SerializeUint(value uint32) []byte {
	byteValue := make([]byte, 4)
	binary.BigEndian.PutUint32(byteValue, value)
	return byteValue
}

func (serializer *Serializer) SerializeString(value string) []byte {
	return []byte(value)
}

func (serializer *Serializer) SerializeToString(dynMap *DynamicMap) string {
	line := strings.Builder{}
	currMap := dynMap.GetCurrentMap()
	for key, value := range currMap {
		if isFloatColumn(key) {
			floatValue := math.Float32frombits(binary.BigEndian.Uint32(value))
			line.WriteString(fmt.Sprintf("%v=%v", key, floatValue))
		} else {
			line.WriteString(fmt.Sprintf("%v=%v", key, string(value)))
		}
		line.WriteString(commaSeparator)
	}
	line.WriteString(newLine)
	return line.String()
}

func (serializer *Serializer) DeserializeFromString(dynMapStr string) *DynamicMap {
	keyValuePairs := strings.Split(dynMapStr, commaSeparator)
	dynMapData := make(map[string][]byte)
	for _, pair := range keyValuePairs {
		keyValuePair := strings.Split(pair, "=")
		key := keyValuePair[0]
		strVal := keyValuePair[1]
		intVal, err := strconv.Atoi(strVal)
		if err != nil {
			floatVal, err := strconv.ParseFloat(strVal, 32)
			if err != nil {
				dynMapData[key] = []byte(strVal)
			} else {
				dynMapData[key] = serializer.SerializeFloat(float32(floatVal))
			}
		} else {
			dynMapData[key] = serializer.SerializeUint(uint32(intVal))
		}
	}
	return NewDynamicMap(dynMapData)
}

func (serializer *Serializer) SerializeFloat(value float32) []byte {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, math.Float32bits(value))
	return bytes
}

func isFloatColumn(key string) bool {
	return key == "totalFare"
}
