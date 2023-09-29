package data_structures

import "encoding/binary"

type DynamicMapSerializer struct{}

func NewDynamicMapSerializer() *DynamicMapSerializer {
	return &DynamicMapSerializer{}
}

func (dynMapSerializer *DynamicMapSerializer) Serialize(dynamicMap *DynamicMap) []byte {
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

func (dynMapSerializer *DynamicMapSerializer) Deserialize(dynamicMapBytes []byte) *DynamicMap {
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
	return NewDynamicMap(mapForDynMap)
}

func (dynMapSerializer *DynamicMapSerializer) SerializeUint(value uint32) []byte {
	byteValue := make([]byte, 4)
	binary.BigEndian.PutUint32(byteValue, value)
	return byteValue
}

func (dynMapSerializer *DynamicMapSerializer) SerializeString(value string) []byte {
	return []byte(value)
}
