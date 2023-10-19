package ex3

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"sort"
)

const indexColumn = 0
const travelDurColumn = 1

// DurationSlice is a type of int matrix with sort interface implemented
type DurationSlice [][]int

// Len is the method used to get the length of the DurationSlice
func (d DurationSlice) Len() int {
	return len(d)
}

// Less compares the second component (Travel Duration as Int) of the sub arrays
func (d DurationSlice) Less(i, j int) bool {

	return d[i][travelDurColumn] < d[j][travelDurColumn]
}

// Swap swaps rows of the duration slice in their order
func (d DurationSlice) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

func DecideWhichRowsToKeep(
	rowsKept [2]*dataStructures.DynamicMap,
	flightRow *dataStructures.DynamicMap,
	saverId int,
) [2]*dataStructures.DynamicMap {
	if rowsKept[1] == nil && rowsKept[0] != nil {
		return [2]*dataStructures.DynamicMap{rowsKept[0], flightRow}
	} else if rowsKept[1] == nil && rowsKept[0] == nil {
		log.Warnf("Saver %v | Journey should not already exist as both parts are nil | Replacing index 0 anyways...", saverId)
		return [2]*dataStructures.DynamicMap{flightRow, nil}
	}
	//Create an array with the rows to facilitate indexation at last step
	arrayOfRows := []*dataStructures.DynamicMap{
		rowsKept[0],
		rowsKept[1],
		flightRow,
	}

	travelDurIdx0, err := rowsKept[0].GetAsInt(utils.ConvertedTravelDuration)
	if err != nil {
		log.Errorf("Saver %v | Error trying to get travelDuration of index 0 in compare | Journey map was: %v | %v", saverId, rowsKept, err)
		return [2]*dataStructures.DynamicMap{rowsKept[0], rowsKept[1]}
	}
	travelDurIdx1, err := rowsKept[1].GetAsInt(utils.ConvertedTravelDuration)
	if err != nil {
		log.Errorf("Saver %v | Error trying to get travelDuration of index 1 in compare | Journey map was: %v | %v", saverId, rowsKept, err)
		return [2]*dataStructures.DynamicMap{rowsKept[0], rowsKept[1]}
	}
	compareTravelDur, err := flightRow.GetAsInt(utils.ConvertedTravelDuration)
	arrayOfDurations := DurationSlice{
		[]int{0, travelDurIdx0},
		[]int{1, travelDurIdx1},
		[]int{2, compareTravelDur},
	}
	sort.Sort(arrayOfDurations)
	return [2]*dataStructures.DynamicMap{arrayOfRows[arrayOfDurations[0][indexColumn]], arrayOfRows[arrayOfDurations[1][indexColumn]]}
}
