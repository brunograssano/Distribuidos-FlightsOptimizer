package parsers

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
)

type Parser interface {
	LineToDynMap(line string) (*dataStructures.DynamicMap, error)

	GetEofMsgType() int

	GetMsgType() int
}
