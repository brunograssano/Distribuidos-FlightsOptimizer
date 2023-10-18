package data_structures

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func TestGetAsAllGetsOfNonExistentColumnShouldThrowError(t *testing.T) {
	dynMap := make(map[string][]byte)
	dynMap["test"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test"], uint32(32))
	row := NewDynamicMap(dynMap)
	_, err := row.GetAsInt("non_existent")
	assert.Error(t, err, "GetAsInt should have thrown error")

	_, err = row.GetAsFloat("non_existent")
	assert.Error(t, err, "GetAsFloat should have thrown error")

	_, err = row.GetAsString("non_existent")
	assert.Error(t, err, "GetAsString should have thrown error")

	_, err = row.GetAsBytes("non_existent")
	assert.Error(t, err, "GetAsBytes should have thrown error")
}

func TestGetAsIntAnIntColumn(t *testing.T) {
	dynMap := make(map[string][]byte)
	dynMap["test"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test"], uint32(32))
	row := NewDynamicMap(dynMap)
	val, err := row.GetAsInt("test")

	assert.Equalf(t, 32, val, "The value saved was different: %v", val)
	assert.Nil(t, err, "Thrown error: %v", err)
}

func TestGetAsFloatAFloat32Column(t *testing.T) {
	dynMap := make(map[string][]byte)
	dynMap["test"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test"], math.Float32bits(32.0))
	row := NewDynamicMap(dynMap)
	val, err := row.GetAsFloat("test")

	assert.Equalf(t, float32(32.0), val, "The value saved was different: %v", val)
	assert.Nil(t, err, "Thrown error: %v", err)
}

func TestGetAsStringAStringColumn(t *testing.T) {
	dynMap := make(map[string][]byte)
	dynMap["test"] = []byte("stringval")
	row := NewDynamicMap(dynMap)
	val, err := row.GetAsString("test")

	assert.Equalf(t, "stringval", val, "The value saved was different: %v", val)
	assert.Nil(t, err, "Thrown error: %v", err)
}

func TestShouldReturnANewRowWithOnlyOneColumnWhenReducingTheRow(t *testing.T) {
	dynMap := make(map[string][]byte)
	const columnToRemove1 = "col1"
	const columnToRemove2 = "col2"
	const columnToKeep = "col3"
	dynMap[columnToRemove1] = []byte("Some data")
	dynMap[columnToKeep] = []byte("More data")
	dynMap[columnToRemove2] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap[columnToRemove2], uint32(5))
	row := NewDynamicMap(dynMap)

	keepCols := []string{columnToKeep}
	newRow, err := row.ReduceToColumns(keepCols)
	assert.Nil(t, err, "Thrown error: %v", err)

	val, err := newRow.GetAsString(columnToKeep)
	assert.Nil(t, err, "Thrown error: %v", err)
	assert.Equalf(t, "More data", val, "The value saved was different: %v", val)

	_, err = newRow.GetAsString(columnToRemove1)
	assert.Errorf(t, err, "It kept another column: %v", columnToRemove1)

	_, err = newRow.GetAsInt(columnToRemove2)
	assert.Errorf(t, err, "It kept another column: %v", columnToRemove2)
}

func TestShouldReturnAnErrorIfAColumnDoesNotExistWhenReducingTheRow(t *testing.T) {
	dynMap := make(map[string][]byte)
	const columnToRemove1 = "col1"
	const columnToRemove2 = "col2"
	const columnToKeepThatIsNotSaved = "col3"
	dynMap[columnToRemove1] = []byte("Some data")
	dynMap[columnToRemove2] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap[columnToRemove2], uint32(5))
	row := NewDynamicMap(dynMap)

	keepCols := []string{columnToKeepThatIsNotSaved}
	_, err := row.ReduceToColumns(keepCols)
	assert.Errorf(t, err, "Should have failed if a non existant column was passed: %v", columnToKeepThatIsNotSaved)
}

func TestGetColumnCountWithZeroColumnsReturnZero(t *testing.T) {
	dynMap := make(map[string][]byte)
	row := NewDynamicMap(dynMap)
	colCount := row.GetColumnCount()
	assert.Equalf(t, uint32(0), colCount, "Column count should be 0 and returned %v", colCount)

}

func TestGetColumnCountWithTwoColumnsShouldReturnTwo(t *testing.T) {
	dynMap := make(map[string][]byte)
	dynMap["test"] = make([]byte, 4)
	dynMap["test_2"] = make([]byte, 4)
	row := NewDynamicMap(dynMap)
	colCount := row.GetColumnCount()
	assert.Equalf(t, uint32(2), colCount, "Column count should be 2 and returned %v", colCount)
}

func TestGetColumnCountWithTwoColumnsShouldReturnTwoAndAfterReduceToOneShouldReturnOne(t *testing.T) {
	dynMap := make(map[string][]byte)
	dynMap["test"] = make([]byte, 4)
	dynMap["test_2"] = make([]byte, 4)
	row := NewDynamicMap(dynMap)
	colCount := row.GetColumnCount()
	assert.Equalf(t, uint32(2), colCount, "Column count should be 2 and returned %v", colCount)

	newRow, err := row.ReduceToColumns([]string{"test"})
	assert.Nilf(t, err, "Thrown error: %v", err)

	colCount = newRow.GetColumnCount()
	assert.Equalf(t, uint32(1), colCount, "Column count should be 1 and returned %v", colCount)
}
