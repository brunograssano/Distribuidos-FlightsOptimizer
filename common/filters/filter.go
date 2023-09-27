package filters

import (
	"errors"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"reflect"
	"strings"
)

type Filter struct {
	FilterInterface
}

func NewFilter() *Filter {
	return &Filter{}
}

func (filter *Filter) Equals(row *dataStructures.DynamicMap, valueOfCompare any, colName string) (bool, error) {
	typeOfData := reflect.TypeOf(valueOfCompare)
	switch typeOfData.Kind() {
	case reflect.Int:
		rowData, err := row.GetAsInt(colName)
		if err != nil {
			return false, err
		}
		compareCasted := valueOfCompare.(int)
		return compareCasted == rowData, nil
	case reflect.String:
		rowData, err := row.GetAsString(colName)
		if err != nil {
			return false, err
		}
		compareCasted := valueOfCompare.(string)
		return compareCasted == rowData, nil
	case reflect.Float32:
		rowData, err := row.GetAsFloat(colName)
		if err != nil {
			return false, err
		}
		compareCasted := valueOfCompare.(float32)
		return compareCasted == rowData, nil
	default:
		return false, errors.New("type of comparison value is not valid")
	}
}

func (filter *Filter) Greater(row *dataStructures.DynamicMap, valueOfCompare any, colName string) (bool, error) {
	typeOfData := reflect.TypeOf(valueOfCompare)
	switch typeOfData.Kind() {
	case reflect.Int:
		rowData, err := row.GetAsInt(colName)
		if err != nil {
			return false, err
		}
		compareCasted := valueOfCompare.(int)
		return compareCasted < rowData, nil
	case reflect.String:
		rowData, err := row.GetAsString(colName)
		if err != nil {
			return false, err
		}
		compareCasted := valueOfCompare.(string)
		return strings.Compare(compareCasted, rowData) < 0, nil
	case reflect.Float32:
		rowData, err := row.GetAsFloat(colName)
		if err != nil {
			return false, err
		}
		compareCasted := valueOfCompare.(float32)
		return compareCasted < rowData, nil
	default:
		return false, errors.New("type of comparison value is not valid")
	}
}

func (filter *Filter) Less(row *dataStructures.DynamicMap, valueOfCompare any, colName string) (bool, error) {
	typeOfData := reflect.TypeOf(valueOfCompare)
	switch typeOfData.Kind() {
	case reflect.Int:
		rowData, err := row.GetAsInt(colName)
		if err != nil {
			return false, err
		}
		compareCasted := valueOfCompare.(int)
		return compareCasted > rowData, nil
	case reflect.String:
		rowData, err := row.GetAsString(colName)
		if err != nil {
			return false, err
		}
		compareCasted := valueOfCompare.(string)
		return strings.Compare(compareCasted, rowData) > 0, nil
	case reflect.Float32:
		rowData, err := row.GetAsFloat(colName)
		if err != nil {
			return false, err
		}
		compareCasted := valueOfCompare.(float32)
		return compareCasted > rowData, nil
	default:
		return false, errors.New("type of comparison value is not valid")
	}
}

func (filter *Filter) GreaterOrEquals(row *dataStructures.DynamicMap, valueOfCompare any, colName string) (bool, error) {
	eq, err := filter.Equals(row, valueOfCompare, colName)
	if err != nil {
		return false, err
	}
	gr, err2 := filter.Greater(row, valueOfCompare, colName)
	if err2 != nil {
		return false, err2
	}

	return eq || gr, nil
}

func (filter *Filter) LessOrEquals(row *dataStructures.DynamicMap, valueOfCompare any, colName string) (bool, error) {
	eq, err := filter.Equals(row, valueOfCompare, colName)
	if err != nil {
		return false, err
	}
	less, err2 := filter.Less(row, valueOfCompare, colName)
	if err2 != nil {
		return false, err2
	}

	return eq || less, nil
}
