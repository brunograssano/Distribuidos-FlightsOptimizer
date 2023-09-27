package data_structures

import (
	"DistribuidosTP1/data_structures"
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
)

func TestSerializeDynMapWithAnIntReturnsTheBytesExpected(t *testing.T) {
	dynMap := make(map[string][]byte)
	dynMap["test"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test"], uint32(32))
	row := data_structures.NewDynamicMap(dynMap)
	serializer := data_structures.NewDynamicMapSerializer()
	serializedRow := serializer.Serialize(row)

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

	if bytes.Compare(bytesExpected, serializedRow) != 0 {
		fmt.Printf("Expected: %v", bytesExpected)
		fmt.Printf("Got: %v", serializedRow)
		t.Errorf("Bytes comparation failed. Expected is not equal to serialized one...")
	}

}

func TestSerializeDynMapWithTwoColumnsReturnsTheBytesExpected(t *testing.T) {
	dynMap := make(map[string][]byte)
	dynMap["test"] = make([]byte, 4)
	stringCol2 := "un string largo para la prueba que debería de igual forma leerse bien su longitud"
	dynMap["test_string"] = []byte(stringCol2)
	binary.BigEndian.PutUint32(dynMap["test"], uint32(32))
	row := data_structures.NewDynamicMap(dynMap)
	serializer := data_structures.NewDynamicMapSerializer()
	serializedRow := serializer.Serialize(row)

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

	if bytes.Compare(bytesExpected, serializedRow) != 0 {
		fmt.Printf("Expected: %v\n", bytesExpected)
		fmt.Printf("Got: %v\n", serializedRow)
		t.Errorf("Bytes comparation failed. Expected is not equal to serialized one...")
	}
}

func TestSerializeEmptyDynMapReturnsOnlyColLengthOfZeroAsBytes(t *testing.T) {
	row := data_structures.NewDynamicMap(make(map[string][]byte))
	serializer := data_structures.NewDynamicMapSerializer()
	serializedRow := serializer.Serialize(row)
	bytesExpected := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesExpected, uint32(0))
	if bytes.Compare(serializedRow, bytesExpected) != 0 {
		t.Errorf("Expected %v and got %v", bytesExpected, serializedRow)
	}
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

	serializer := data_structures.NewDynamicMapSerializer()
	rowReceived := serializer.Deserialize(bytesSer)

	stringCol, errStringCol := rowReceived.GetAsString("test_string")
	if errStringCol != nil {
		t.Errorf("Got error on casting string col that was string in original.")
	}
	if strings.Compare(stringCol, stringCol2) != 0 {
		t.Errorf("Wrong value on stringCol expected %v and got %v.", stringCol2, stringCol)
	}
	intCol, errIntCol := rowReceived.GetAsInt("test")
	if errIntCol != nil {
		t.Errorf("Got error on casting int col that was int in original.")
	}
	if intCol != 32 {
		t.Errorf("Wrong value on intCol expected 32 and got %v.", intCol)
	}
	if rowReceived.GetColumnCount() != 2 {
		t.Errorf("RowCount expected was 2 and got %v.", rowReceived.GetColumnCount())
	}
}
