package filters

import "DistribuidosTP1/data_structures"

type FilterInterface interface {
	Equals(row data_structures.FlightRow, valueOfCompare interface{}, colName string) (bool, error)
	GreaterThan(row data_structures.FlightRow, valueOfCompare interface{}, colName string) (bool, error)
	LessThan(row data_structures.FlightRow, valueOfCompare interface{}, colName string) (bool, error)
	GreaterOrEquals(row data_structures.FlightRow, valueOfCompare interface{}, colName string) (bool, error)
	LessOrEquals(row data_structures.FlightRow, valueOfCompare interface{}, colName string) (bool, error)
}
