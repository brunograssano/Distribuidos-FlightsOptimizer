package serializer

import (
	"encoding/binary"
	"math"
)

func SerializeUint(value uint32) []byte {
	byteValue := make([]byte, 4)
	binary.BigEndian.PutUint32(byteValue, value)
	return byteValue
}

func SerializeString(value string) []byte {
	return []byte(value)
}

func SerializeFloat(value float32) []byte {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, math.Float32bits(value))
	return bytes
}
