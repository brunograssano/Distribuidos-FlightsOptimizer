package serializer

import (
	"bytes"
	"encoding/binary"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestSerializeDynMapWithAnIntReturnsTheBytesExpected(t *testing.T) {
	dynMap := make(map[string][]byte)
	dynMap["test"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test"], uint32(32))
	row := dataStructures.NewDynamicMap(dynMap)

	serializedRow := SerializeDynMap(row)

	var bytesExpected []byte
	bytesNCols := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesNCols, uint32(1))
	bytesLenKey := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesLenKey, uint32(len("test")))
	bytesKey := []byte("test")
	bytesLenValue := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesLenValue, uint32(4))
	bytesValue := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesValue, uint32(32))
	bytesExpected = append(bytesExpected, bytesNCols...)
	bytesExpected = append(bytesExpected, bytesLenKey...)
	bytesExpected = append(bytesExpected, bytesKey...)
	bytesExpected = append(bytesExpected, bytesLenValue...)
	bytesExpected = append(bytesExpected, bytesValue...)

	assert.Zerof(t, bytes.Compare(bytesExpected, serializedRow), "Expected is not equal to serialized one | Expected: %v, Got: %v", bytesExpected, serializedRow)
}

func TestSerializeDynMapWithTwoColumnsReturnsTheBytesExpected(t *testing.T) {
	dynMap := make(map[string][]byte)
	dynMap["test"] = make([]byte, 4)
	stringCol2 := "un string largo para la prueba que debería de igual forma leerse bien su longitud"
	dynMap["test_string"] = []byte(stringCol2)
	binary.BigEndian.PutUint32(dynMap["test"], uint32(32))
	row := dataStructures.NewDynamicMap(dynMap)

	serializedRow := SerializeDynMap(row)

	var bytesExpected []byte
	bytesNCols := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesNCols, uint32(2))
	bytesLenKey := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesLenKey, uint32(len("test")))
	bytesKey := []byte("test")
	bytesLenValue := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesLenValue, uint32(4))
	bytesValue := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesValue, uint32(32))
	bytesLenKey2 := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesLenKey2, uint32(len("test_string")))
	bytesKey2 := []byte("test_string")
	bytesLenValue2 := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesLenValue2, uint32(len(stringCol2)))
	bytesValue2 := []byte(stringCol2)
	bytesExpected = append(bytesExpected, bytesNCols...)
	bytesExpected = append(bytesExpected, bytesLenKey...)
	bytesExpected = append(bytesExpected, bytesKey...)
	bytesExpected = append(bytesExpected, bytesLenValue...)
	bytesExpected = append(bytesExpected, bytesValue...)
	bytesExpected = append(bytesExpected, bytesLenKey2...)
	bytesExpected = append(bytesExpected, bytesKey2...)
	bytesExpected = append(bytesExpected, bytesLenValue2...)
	bytesExpected = append(bytesExpected, bytesValue2...)

	assert.Zerof(t, bytes.Compare(bytesExpected, serializedRow), "Expected is not equal to serialized one | Expected: %v, Got: %v", bytesExpected, serializedRow)

}

func TestSerializeEmptyDynMapReturnsOnlyColLengthOfZeroAsBytes(t *testing.T) {
	row := dataStructures.NewDynamicMap(make(map[string][]byte))
	serializedRow := SerializeDynMap(row)
	bytesExpected := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesExpected, uint32(0))
	assert.Zerof(t, bytes.Compare(bytesExpected, serializedRow), "Expected is not equal to serialized one | Expected: %v, Got: %v", bytesExpected, serializedRow)

}

func TestDeserializeDynMapWithTwoColumnsReturnsTheExpectedDynMap(t *testing.T) {

	dynMapExpected := make(map[string][]byte)
	dynMapExpected["test"] = make([]byte, 4)
	stringCol2 := "un string largo para la prueba que debería de igual forma leerse bien su longitud"
	dynMapExpected["test_string"] = []byte(stringCol2)
	binary.BigEndian.PutUint32(dynMapExpected["test"], uint32(32))

	var bytesSer []byte
	bytesNCols := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesNCols, uint32(2))
	bytesLenKey := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesLenKey, uint32(len("test")))
	bytesKey := []byte("test")
	bytesLenValue := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesLenValue, uint32(4))
	bytesValue := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesValue, uint32(32))
	bytesLenKey2 := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesLenKey2, uint32(len("test_string")))
	bytesKey2 := []byte("test_string")
	bytesLenValue2 := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesLenValue2, uint32(len(stringCol2)))
	bytesValue2 := []byte(stringCol2)
	bytesSer = append(bytesSer, bytesNCols...)
	bytesSer = append(bytesSer, bytesLenKey...)
	bytesSer = append(bytesSer, bytesKey...)
	bytesSer = append(bytesSer, bytesLenValue...)
	bytesSer = append(bytesSer, bytesValue...)
	bytesSer = append(bytesSer, bytesLenKey2...)
	bytesSer = append(bytesSer, bytesKey2...)
	bytesSer = append(bytesSer, bytesLenValue2...)
	bytesSer = append(bytesSer, bytesValue2...)

	rowReceived, _ := DeserializeDynMap(bytesSer)

	stringCol, errStringCol := rowReceived.GetAsString("test_string")
	assert.Nil(t, errStringCol, "Got error on casting string col that was string in original.")
	assert.Zerof(t, strings.Compare(stringCol, stringCol2), "Wrong value on stringCol expected %v and got %v.", stringCol2, stringCol)

	intCol, errIntCol := rowReceived.GetAsInt("test")
	assert.Nil(t, errIntCol, "Got error on casting int col that was int in original.")
	assert.Equalf(t, 32, intCol, "Wrong value on intCol expected 32 and got %v.", intCol)
	assert.Equalf(t, uint32(2), rowReceived.GetColumnCount(), "RowCount expected was 2 and got %v.", rowReceived.GetColumnCount())
}
