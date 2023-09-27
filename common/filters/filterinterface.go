package filters

import dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"

type FilterInterface interface {
	Equals(row dataStructures.DynamicMap, valueOfCompare interface{}, colName string) (bool, error)
	GreaterThan(row dataStructures.DynamicMap, valueOfCompare interface{}, colName string) (bool, error)
	LessThan(row dataStructures.DynamicMap, valueOfCompare interface{}, colName string) (bool, error)
	GreaterOrEquals(row dataStructures.DynamicMap, valueOfCompare interface{}, colName string) (bool, error)
	LessOrEquals(row dataStructures.DynamicMap, valueOfCompare interface{}, colName string) (bool, error)
}
