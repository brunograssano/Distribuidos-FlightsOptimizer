package data_structures

import (
	"encoding/binary"
	"fmt"
	"math"
)

type DynamicMap struct {
	cols map[string][]byte
}

func NewDynamicMap(dynMap map[string][]byte) *DynamicMap {
	return &DynamicMap{
		cols: dynMap,
	}
}

func (dm *DynamicMap) GetAsInt(colName string) (int, error) {
	data, exists := dm.cols[colName]
	if !exists {
		return 0, fmt.Errorf("column %v does not exists", colName)
	}
	intValue := int(binary.BigEndian.Uint32(data))
	return intValue, nil
}

func (dm *DynamicMap) GetAsFloat(colName string) (float32, error) {
	data, exists := dm.cols[colName]
	if !exists {
		return 0, fmt.Errorf("column %v does not exists", colName)
	}
	floatValue := math.Float32frombits(binary.BigEndian.Uint32(data))
	return floatValue, nil
}

func (dm *DynamicMap) GetAsString(colName string) (string, error) {
	data, exists := dm.cols[colName]
	if !exists {
		return "", fmt.Errorf("column %v does not exists", colName)
	}
	strVal := string(data)
	return strVal, nil
}

// GetAsAny
/*
	Gets the data as an interface, user should get type of interface afterward
	or use reflect library to know the dynamic data type and cast it.

	Returns error on lack of existence of the column
*/
func (dm *DynamicMap) GetAsAny(colName string) (interface{}, error) {
	data, exists := dm.cols[colName]
	if !exists {
		return nil, fmt.Errorf("column %v does not exist", colName)
	}
	return data, nil
}

// ReduceToColumns
/*
	Returns a new flight row with only the columns passed as an argument
	Returns error if a column does not exist
*/
func (dm *DynamicMap) ReduceToColumns(columns []string) (*DynamicMap, error) {
	reducedRow := make(map[string][]byte)
	for _, col := range columns {
		data, exists := dm.cols[col]
		if !exists {
			return nil, fmt.Errorf("column %v does not exist", col)
		}
		reducedRow[col] = data
	}
	return NewDynamicMap(reducedRow), nil
}

func (dm *DynamicMap) GetColumnCount() uint32 {
	totalLong := 0
	for _, _ = range dm.cols {
		totalLong += 1
	}
	return uint32(totalLong)
}

func (dm *DynamicMap) GetCurrentMap() map[string][]byte {
	return dm.cols
}

func (dm *DynamicMap) AddColumn(key string, value []byte) {
	dm.cols[key] = value
}
