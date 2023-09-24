package data_structures

import (
	"errors"
	"fmt"
	"strconv"
)

type FlightRow struct {
	cols map[string]interface{}
}

func NewFlightRow(dynMap map[string]interface{}) *FlightRow {
	return &FlightRow{
		cols: dynMap,
	}
}

func (fr *FlightRow) GetAsInt(colName string) (int, error) {
	intValue, ok := fr.cols[colName].(int)
	if !ok {
		strValue, okStr := fr.cols[colName].(string)
		if !okStr {
			floatValue, okFloat := fr.cols[colName].(float32)
			if !okFloat {
				return 0, errors.New("column could not be gotten as int")
			}
			return int(floatValue), nil
		}
		return strconv.Atoi(strValue)
	}
	return intValue, nil
}

func (fr *FlightRow) GetAsFloat(colName string) (float32, error) {
	floatValue, ok := fr.cols[colName].(float32)
	if !ok {
		strValue, okStr := fr.cols[colName].(string)
		if !okStr {
			intValue, okInt := fr.cols[colName].(int)
			if !okInt {
				return 0, errors.New("column could not be gotten as float")
			}
			return float32(intValue), nil
		}
		floatVal, errStr := strconv.ParseFloat(strValue, 32)
		if errStr != nil {
			return 0.0, errStr
		}
		return float32(floatVal), nil
	}
	return floatValue, nil
}

func (fr *FlightRow) GetAsString(colName string) (string, error) {
	strVal, ok := fr.cols[colName].(string)
	if !ok {
		intVal, okInt := fr.cols[colName].(int)
		if !okInt {
			floatVal, okInt := fr.cols[colName].(float32)
			if !okInt {
				return "", errors.New("column could not be gotten as string")
			}
			return fmt.Sprintf("%v", floatVal), nil
		}
		return fmt.Sprintf("%v", intVal), nil
	}
	return strVal, nil
}

// GetAsAny
/*
	Gets the data as an interface, user should get type of interface afterward
	or use reflect library to know the dynamic data type and cast it.

	Returns error on lack of existence of the column
*/
func (fr *FlightRow) GetAsAny(colName string) (interface{}, error) {
	data, exists := fr.cols[colName]
	if !exists {
		return nil, errors.New("column does not exist")
	}
	return data, nil
}
