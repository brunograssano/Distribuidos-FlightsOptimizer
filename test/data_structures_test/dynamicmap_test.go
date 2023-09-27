package data_structures_test

import (
	"DistribuidosTP1/data_structures"
	"encoding/binary"
	"math"
	"testing"
)

func TestGetAsAllGetsOfNonExistentColumnShouldThrowError(t *testing.T) {
	dynMap := make(map[string][]byte)
	dynMap["test"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test"], uint32(32))
	row := data_structures.NewDynamicMap(dynMap)
	_, err := row.GetAsInt("non_existent")
	if err == nil {
		t.Errorf("GetAsInt should have thrown error")
	}
	_, err = row.GetAsFloat("non_existent")
	if err == nil {
		t.Errorf("GetAsFloat should have thrown error")
	}
	_, err = row.GetAsString("non_existent")
	if err == nil {
		t.Errorf("GetAsString should have thrown error")
	}
	_, err = row.GetAsAny("non_existent")
	if err == nil {
		t.Errorf("GetAsAny should have thrown error")
	}
}

func TestGetAsIntAnIntColumn(t *testing.T) {
	dynMap := make(map[string][]byte)
	dynMap["test"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test"], uint32(32))
	row := data_structures.NewDynamicMap(dynMap)
	val, err := row.GetAsInt("test")
	if val != 32 {
		t.Errorf("Value %v is not equal to 32", val)
	}
	if err != nil {
		t.Errorf("Thrown error: %v", err)
	}
}

func TestGetAsFloatAFloat32Column(t *testing.T) {
	dynMap := make(map[string][]byte)
	dynMap["test"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test"], math.Float32bits(32.0))
	row := data_structures.NewDynamicMap(dynMap)
	val, err := row.GetAsFloat("test")
	if val != 32.0 {
		t.Errorf("Value %v is not equal to 32.0", val)
	}
	if err != nil {
		t.Errorf("Thrown error: %v", err)
	}
}

func TestGetAsStringAStringColumn(t *testing.T) {
	dynMap := make(map[string][]byte)
	dynMap["test"] = []byte("stringval")
	row := data_structures.NewDynamicMap(dynMap)
	val, err := row.GetAsString("test")
	if val != "stringval" {
		t.Errorf("Value %v is not equal to stringval", val)
	}
	if err != nil {
		t.Errorf("Thrown error: %v", err)
	}
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
	row := data_structures.NewDynamicMap(dynMap)

	keepCols := []string{columnToKeep}
	newRow, err := row.ReduceToColumns(keepCols)
	if err != nil {
		t.Errorf("Thrown error: %v", err)
	}
	val, err := newRow.GetAsString(columnToKeep)
	if err != nil {
		t.Errorf("Thrown error: %v", err)
	}
	if val != "More data" {
		t.Errorf("The value saved was different: %v", val)
	}
	_, err = newRow.GetAsString(columnToRemove1)
	if err == nil {
		t.Errorf("It kept another column: %v", columnToRemove1)
	}
	_, err = newRow.GetAsInt(columnToRemove2)
	if err == nil {
		t.Errorf("It kept another column: %v", columnToRemove2)
	}
}

func TestShouldReturnAnErrorIfAColumnDoesNotExistWhenReducingTheRow(t *testing.T) {
	dynMap := make(map[string][]byte)
	const columnToRemove1 = "col1"
	const columnToRemove2 = "col2"
	const columnToKeepThatIsNotSaved = "col3"
	dynMap[columnToRemove1] = []byte("Some data")
	dynMap[columnToRemove2] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap[columnToRemove2], uint32(5))
	row := data_structures.NewDynamicMap(dynMap)

	keepCols := []string{columnToKeepThatIsNotSaved}
	newRow, err := row.ReduceToColumns(keepCols)
	if err == nil || newRow != nil {
		t.Errorf("Should have failed if a non existant column was passed: %v", columnToKeepThatIsNotSaved)
	}
}

func TestGetColumnCountWithZeroColumnsReturnZero(t *testing.T) {
	dynMap := make(map[string][]byte)
	row := data_structures.NewDynamicMap(dynMap)
	colCount := row.GetColumnCount()
	if colCount != 0 {
		t.Errorf("Column count should be 0 and returned %v", colCount)
	}
}

func TestGetColumnCountWithTwoColumnsShouldReturnTwo(t *testing.T) {
	dynMap := make(map[string][]byte)
	dynMap["test"] = make([]byte, 4)
	dynMap["test_2"] = make([]byte, 4)
	row := data_structures.NewDynamicMap(dynMap)
	colCount := row.GetColumnCount()
	if colCount != 2 {
		t.Errorf("Column count should be 2 and returned %v", colCount)
	}
}

func TestGetColumnCountWithTwoColumnsShouldReturnTwoAndAfterReduceToOneShouldReturnOne(t *testing.T) {
	dynMap := make(map[string][]byte)
	dynMap["test"] = make([]byte, 4)
	dynMap["test_2"] = make([]byte, 4)
	row := data_structures.NewDynamicMap(dynMap)
	colCount := row.GetColumnCount()
	if colCount != 2 {
		t.Errorf("Column count should be 2 and returned %v", colCount)
	}

	newRow, err := row.ReduceToColumns([]string{"test"})
	if err != nil {
		t.Errorf("Thrown error: %v", err)
	}
	colCount = newRow.GetColumnCount()
	if colCount != 1 {
		t.Errorf("Column count should be 1 and returned %v", colCount)
	}
}
