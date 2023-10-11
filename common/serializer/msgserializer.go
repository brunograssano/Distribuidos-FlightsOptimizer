package serializer

import (
	"encoding/binary"
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"math"
	"strconv"
	"strings"
)

func SerializeMsg(msg *data_structures.Message) []byte {
	serializedMsg := []byte{}
	typeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(typeBytes, uint32(msg.TypeMessage))
	nRows := make([]byte, 4)
	binary.BigEndian.PutUint32(nRows, uint32(len(msg.DynMaps)))
	serializedMsg = append(serializedMsg, typeBytes...)
	serializedMsg = append(serializedMsg, nRows...)
	for _, row := range msg.DynMaps {
		serializedRow := SerializeDynMap(row)
		serializedMsg = append(serializedMsg, serializedRow...)
	}
	return serializedMsg
}

func DeserializeMsg(bytesMsg []byte) *data_structures.Message {
	offset := 0
	typeMsg := int(binary.BigEndian.Uint32(bytesMsg[offset : offset+4]))
	offset = 4
	nRows := int(binary.BigEndian.Uint32(bytesMsg[offset : offset+4]))
	offset = 8
	var dynMaps []*data_structures.DynamicMap
	for i := 0; i < nRows; i++ {
		dynMap, bytesRead := DeserializeDynMap(bytesMsg[offset:])
		dynMaps = append(dynMaps, dynMap)
		offset += bytesRead
	}
	return &data_structures.Message{
		TypeMessage: typeMsg,
		DynMaps:     dynMaps,
	}
}

func SerializeDynMap(dynamicMap *data_structures.DynamicMap) []byte {
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

func DeserializeDynMap(dynamicMapBytes []byte) (*data_structures.DynamicMap, int) {
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
	return data_structures.NewDynamicMap(mapForDynMap), currOffset
}

func SerializeToString(dynMap *data_structures.DynamicMap) string {
	line := strings.Builder{}
	currMap := dynMap.GetCurrentMap()
	currCol := 0
	colCount := dynMap.GetColumnCount()
	for key, value := range currMap {
		if isFloatColumn(key) {
			floatValue := math.Float32frombits(binary.BigEndian.Uint32(value))
			line.WriteString(fmt.Sprintf("%v=%v", key, floatValue))
		} else if isIntColumn(key) {
			intValue := binary.BigEndian.Uint32(value)
			line.WriteString(fmt.Sprintf("%v=%v", key, intValue))
		} else {
			line.WriteString(fmt.Sprintf("%v=%v", key, string(value)))
		}
		if uint32(currCol) != colCount-1 {
			line.WriteString(utils.CommaSeparator)
			currCol++
		}
	}
	line.WriteString(utils.NewLine)
	return line.String()
}

func DeserializeFromString(dynMapStr string) *data_structures.DynamicMap {
	keyValuePairs := strings.Split(dynMapStr, utils.CommaSeparator)
	dynMapData := make(map[string][]byte)
	for _, pair := range keyValuePairs {
		keyValuePair := strings.Split(pair, "=")
		key := keyValuePair[0]
		strVal := keyValuePair[1]
		if isIntColumn(key) {
			intVal, err := strconv.Atoi(strVal)
			if err != nil {
				log.Errorf("Serializer | Error casting column %v to integer | %v", intVal, err)
			}
			dynMapData[key] = SerializeUint(uint32(intVal))
		} else if isFloatColumn(key) {
			floatVal, err := strconv.ParseFloat(strVal, 32)
			if err != nil {
				log.Errorf("Serializer | Error casting column %v to float | %v", floatVal, err)
			}
			dynMapData[key] = SerializeFloat(float32(floatVal))
		} else {
			dynMapData[key] = []byte(strVal)
		}
	}
	return data_structures.NewDynamicMap(dynMapData)
}
