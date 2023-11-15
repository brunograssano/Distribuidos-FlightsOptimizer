package serializer

import (
	"encoding/binary"
	"fmt"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"math"
	"strconv"
	"strings"
)

func SerializeMsg(msg *dataStructures.Message) []byte {
	var serializedMsg []byte
	typeBytes := SerializeUint(uint32(msg.TypeMessage))
	nRows := SerializeUint(uint32(len(msg.DynMaps)))
	sizeUuidBytes := SerializeUint(uint32(len(msg.ClientId)))
	uuidBytes := SerializeString(msg.ClientId)
	serializedMsg = append(serializedMsg, typeBytes...)
	serializedMsg = append(serializedMsg, nRows...)
	serializedMsg = append(serializedMsg, sizeUuidBytes...)
	serializedMsg = append(serializedMsg, uuidBytes...)
	for _, row := range msg.DynMaps {
		serializedRow := SerializeDynMap(row)
		serializedMsg = append(serializedMsg, serializedRow...)
	}
	return serializedMsg
}

func DeserializeMsg(bytesMsg []byte) *dataStructures.Message {
	offset := 0
	typeMsg := int(binary.BigEndian.Uint32(bytesMsg[offset : offset+4]))
	offset += 4
	nRows := int(binary.BigEndian.Uint32(bytesMsg[offset : offset+4]))
	offset += 4
	sizeOfClientId := int(binary.BigEndian.Uint32(bytesMsg[offset : offset+4]))
	offset += 4
	clientId := DeserializeString(bytesMsg[offset : offset+sizeOfClientId])
	offset += sizeOfClientId
	var dynMaps []*dataStructures.DynamicMap
	for i := 0; i < nRows; i++ {
		dynMap, bytesRead := DeserializeDynMap(bytesMsg[offset:])
		dynMaps = append(dynMaps, dynMap)
		offset += bytesRead
	}
	return &dataStructures.Message{
		TypeMessage: typeMsg,
		DynMaps:     dynMaps,
		ClientId:    clientId,
	}
}

func SerializeDynMap(dynamicMap *dataStructures.DynamicMap) []byte {
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

func DeserializeDynMap(dynamicMapBytes []byte) (*dataStructures.DynamicMap, int) {
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
	return dataStructures.NewDynamicMap(mapForDynMap), currOffset
}

func SerializeToString(dynMap *dataStructures.DynamicMap) string {
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

func DeserializeFromString(dynMapStr string) *dataStructures.DynamicMap {
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
	return dataStructures.NewDynamicMap(dynMapData)
}

func DeserializeUDPPacket(packetBytes []byte) *dataStructures.UDPPacket {
	return &dataStructures.UDPPacket{
		PacketType: packetBytes[0],
		NodeID:     packetBytes[1],
	}
}

func SerializeUDPPacket(packet *dataStructures.UDPPacket) []byte {
	udpPacketBytes := make([]byte, 2)
	udpPacketBytes[0] = packet.PacketType
	udpPacketBytes[1] = packet.NodeID
	return udpPacketBytes
}
