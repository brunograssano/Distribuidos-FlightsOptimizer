package filters

import (
	"encoding/binary"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func TestFilterEqualsShouldThrowErrorWhenColumnNotFound(t *testing.T) {
	expectedString := "test_string"
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = []byte("test_string")
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.Equals(row, expectedString+"_shall not pass", "test_column_not_ex")

	assert.False(t, retVal, "Return value should be false and it was true")
	assert.Error(t, err, "Filter Equals should have thrown error because col does not exist.")
}

func TestFilterEqualsWithString(t *testing.T) {
	expectedString := "test_string"
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = []byte("test_string")
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.Equals(row, expectedString, "test_column")

	assert.True(t, retVal, "Return value should be true and it was false")
	assert.Nilf(t, err, "Filter Equals should not have thrown error. Error was: %v", err)
}

func TestFilterEqualsIsFalseWithString(t *testing.T) {
	expectedString := "test_string"
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = []byte(expectedString)
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.Equals(row, expectedString+"_shall not pass", "test_column")

	assert.False(t, retVal, "Return value should be false and it was true")
	assert.Nilf(t, err, "Filter Equals should have thrown error. Error was: %v", err)
}

func TestFilterGreaterThanWithStringIsTrue(t *testing.T) {
	expectedString := "string"
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = []byte(expectedString)
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.Greater(row, "strinf", "test_column")

	assert.True(t, retVal, "Return value should be true and it was false")
	assert.Nilf(t, err, "Filter Equals should not have thrown error. Error was: %v", err)
}

func TestFilterGreaterThanWithStringIsFalse(t *testing.T) {
	expectedString := "string"
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = []byte(expectedString)
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.Greater(row, "string", "test_column")
	assert.False(t, retVal, "Return value should be false and it was true")
	assert.Nil(t, err, "Filter Equals should not have thrown error. Error was: %v", err)

	retVal, err = filter.Greater(row, "strinh", "test_column")
	assert.False(t, retVal, "Return value should be false and it was true")
	assert.Nil(t, err, "Filter Equals should not have thrown error. Error was: %v", err)
}

func TestFilterLessThanWithStringIsTrue(t *testing.T) {
	expectedString := "string"
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = []byte(expectedString)
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.Less(row, "strinh", "test_column")

	assert.True(t, retVal, "Return value should be true and it was false")
	assert.Nilf(t, err, "Filter Equals should not have thrown error. Error was: %v", err)
}

func TestFilterLessThanWithStringIsFalse(t *testing.T) {
	expectedString := "string"
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = []byte(expectedString)
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.Less(row, "string", "test_column")
	assert.False(t, retVal, "Return value should be false and it was true")
	assert.Nil(t, err, "Filter Equals should not have thrown error. Error was: %v", err)

	retVal, err = filter.Less(row, "strinf", "test_column")
	assert.False(t, retVal, "Return value should be false and it was true")
	assert.Nil(t, err, "Filter Equals should not have thrown error. Error was: %v", err)
}

func TestFilterLessOrEqualThanWithStringIsTrue(t *testing.T) {
	expectedString := "string"
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = []byte(expectedString)
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.LessOrEquals(row, "strinh", "test_column")
	assert.True(t, retVal, "Return value should be true and it was false")
	assert.Nilf(t, err, "Filter Equals should not have thrown error. Error was: %v", err)

	retVal, err = filter.LessOrEquals(row, "string", "test_column")
	assert.True(t, retVal, "Return value should be true and it was false")
	assert.Nilf(t, err, "Filter Equals should not have thrown error. Error was: %v", err)
}

func TestFilterLessOrEqualThanWithStringIsFalse(t *testing.T) {
	expectedString := "string"
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = []byte(expectedString)
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.LessOrEquals(row, "strinf", "test_column")
	assert.False(t, retVal, "Return value should be false and it was true")
	assert.Nil(t, err, "Filter Equals should not have thrown error. Error was: %v", err)
}

func TestFilterGreaterOrEqualThanWithStringIsTrue(t *testing.T) {
	expectedString := "string"
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = []byte(expectedString)
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.GreaterOrEquals(row, "strinf", "test_column")
	assert.True(t, retVal, "Return value should be true and it was false")
	assert.Nilf(t, err, "Filter Equals should not have thrown error. Error was: %v", err)

	retVal, err = filter.GreaterOrEquals(row, "string", "test_column")
	assert.True(t, retVal, "Return value should be true and it was false")
	assert.Nilf(t, err, "Filter Equals should not have thrown error. Error was: %v", err)
}

func TestFilterGreaterOrEqualThanWithStringIsFalse(t *testing.T) {
	expectedString := "string"
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = []byte(expectedString)
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.GreaterOrEquals(row, "strinh", "test_column")
	assert.False(t, retVal, "Return value should be false and it was true")
	assert.Nil(t, err, "Filter Equals should not have thrown error. Error was: %v", err)
}

func TestFilterEqualsWithFloatIsTrue(t *testing.T) {
	expectedFloat := float32(5.3252)
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test_column"], math.Float32bits(expectedFloat))
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.Equals(row, expectedFloat, "test_column")
	assert.True(t, retVal, "Return value should be true and it was false")
	assert.Nilf(t, err, "Filter Equals should not have thrown error. Error was: %v", err)
}

func TestFilterEqualsWithFloatIsFalse(t *testing.T) {
	expectedFloat := float32(5.3252)
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test_column"], math.Float32bits(expectedFloat+1.5432))
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.Equals(row, expectedFloat, "test_column")
	assert.False(t, retVal, "Return value should be false and it was true")
	assert.Nil(t, err, "Filter Equals should not have thrown error. Error was: %v", err)
}

func TestFilterGreaterThanWithFloatIsTrue(t *testing.T) {
	expectedFloat := float32(6.1234)
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test_column"], math.Float32bits(expectedFloat))
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.Greater(row, expectedFloat-2, "test_column")
	assert.True(t, retVal, "Return value should be true and it was false")
	assert.Nilf(t, err, "Filter Equals should not have thrown error. Error was: %v", err)
}

func TestFilterGreaterThanWithFloatIsFalse(t *testing.T) {
	expectedFloat := float32(6.4242)
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test_column"], math.Float32bits(expectedFloat))
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.Greater(row, expectedFloat, "test_column")
	assert.False(t, retVal, "Return value should be false and it was true")
	assert.Nil(t, err, "Filter Equals should not have thrown error. Error was: %v", err)

	retVal, err = filter.Greater(row, expectedFloat+2, "test_column")
	assert.False(t, retVal, "Return value should be false and it was true")
	assert.Nil(t, err, "Filter Equals should not have thrown error. Error was: %v", err)
}

func TestFilterLessThanWithFloatIsTrue(t *testing.T) {
	expectedFloat := float32(6.1234)
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test_column"], math.Float32bits(expectedFloat))
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.Less(row, expectedFloat+2, "test_column")
	assert.True(t, retVal, "Return value should be true and it was false")
	assert.Nilf(t, err, "Filter Equals should not have thrown error. Error was: %v", err)
}

func TestFilterLessThanWithFloatIsFalse(t *testing.T) {
	expectedFloat := float32(6.4242)
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test_column"], math.Float32bits(expectedFloat))
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.Less(row, expectedFloat, "test_column")
	assert.False(t, retVal, "Return value should be false and it was true")
	assert.Nil(t, err, "Filter Equals should not have thrown error. Error was: %v", err)
	retVal, err = filter.Less(row, expectedFloat-2, "test_column")
	assert.False(t, retVal, "Return value should be false and it was true")
	assert.Nil(t, err, "Filter Equals should not have thrown error. Error was: %v", err)
}

func TestFilterLessOrEqualWithFloatIsTrue(t *testing.T) {
	expectedFloat := float32(6.1234)
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test_column"], math.Float32bits(expectedFloat))
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.LessOrEquals(row, expectedFloat+2, "test_column")
	assert.True(t, retVal, "Return value should be true and it was false")
	assert.Nilf(t, err, "Filter Equals should not have thrown error. Error was: %v", err)

	retVal, err = filter.LessOrEquals(row, expectedFloat, "test_column")
	assert.True(t, retVal, "Return value should be true and it was false")
	assert.Nilf(t, err, "Filter Equals should not have thrown error. Error was: %v", err)
}

func TestFilterLessOrEqualWithFloatIsFalse(t *testing.T) {
	expectedFloat := float32(6.4242)
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test_column"], math.Float32bits(expectedFloat))
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.LessOrEquals(row, expectedFloat-2, "test_column")
	assert.False(t, retVal, "Return value should be false and it was true")
	assert.Nil(t, err, "Filter Equals should not have thrown error. Error was: %v", err)
}

func TestFilterGreaterOrEqualWithFloatIsTrue(t *testing.T) {
	expectedFloat := float32(6.1234)
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test_column"], math.Float32bits(expectedFloat))
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.GreaterOrEquals(row, expectedFloat-2, "test_column")
	assert.True(t, retVal, "Return value should be true and it was false")
	assert.Nilf(t, err, "Filter Equals should not have thrown error. Error was: %v", err)

	retVal, err = filter.GreaterOrEquals(row, expectedFloat, "test_column")
	assert.True(t, retVal, "Return value should be true and it was false")
	assert.Nilf(t, err, "Filter Equals should not have thrown error. Error was: %v", err)
}

func TestFilterGreaterOrEqualWithFloatIsFalse(t *testing.T) {
	expectedFloat := float32(6.4242)
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test_column"], math.Float32bits(expectedFloat))
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.GreaterOrEquals(row, expectedFloat+2, "test_column")
	assert.False(t, retVal, "Return value should be false and it was true")
	assert.Nil(t, err, "Filter Equals should not have thrown error. Error was: %v", err)
}

func TestFilterEqualsWithIntIsTrue(t *testing.T) {
	expectedInt := 6
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test_column"], uint32(expectedInt))
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.Equals(row, expectedInt, "test_column")

	assert.True(t, retVal, "Return value should be true and it was false")
	assert.Nilf(t, err, "Filter Equals should not have thrown error. Error was: %v", err)
}

func TestFilterEqualsWithIntIsFalse(t *testing.T) {
	expectedInt := 6
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test_column"], uint32(expectedInt+1))
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.Equals(row, expectedInt, "test_column")
	assert.False(t, retVal, "Return value should be false and it was true")
	assert.Nil(t, err, "Filter Equals should not have thrown error. Error was: %v", err)
}

func TestFilterGreaterThanWithIntIsTrue(t *testing.T) {
	expectedInt := 6
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test_column"], uint32(expectedInt))
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.Greater(row, expectedInt-2, "test_column")

	assert.True(t, retVal, "Return value should be true and it was false")
	assert.Nilf(t, err, "Filter Equals should not have thrown error. Error was: %v", err)
}

func TestFilterGreaterThanWithIntIsFalse(t *testing.T) {
	expectedInt := 6
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test_column"], uint32(expectedInt))
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)

	retVal, err := filter.Greater(row, expectedInt, "test_column")
	assert.False(t, retVal, "Return value should be false and it was true")
	assert.Nil(t, err, "Filter Equals should not have thrown error. Error was: %v", err)

	retVal, err = filter.Greater(row, expectedInt+2, "test_column")
	assert.False(t, retVal, "Return value should be false and it was true")
	assert.Nil(t, err, "Filter Equals should not have thrown error. Error was: %v", err)
}

func TestFilterLessThanWithIntIsTrue(t *testing.T) {
	expectedInt := 6
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test_column"], uint32(expectedInt))
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.Less(row, expectedInt+2, "test_column")
	assert.True(t, retVal, "Return value should be true and it was false")
	assert.Nilf(t, err, "Filter Less should not have thrown error. Error was: %v", err)
}

func TestFilterLessThanWithIntIsFalse(t *testing.T) {
	expectedInt := 6
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test_column"], uint32(expectedInt))
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.Less(row, expectedInt, "test_column")
	assert.False(t, retVal, "Return value equal should be false and it was true")
	assert.Nilf(t, err, "Filter Less should not have thrown error. Error was: %v", err)

	retVal, err = filter.Less(row, expectedInt-2, "test_column")
	assert.False(t, retVal, "Return value lower than should be false and it was true")
	assert.Nilf(t, err, "Filter Less should not have thrown error. Error was: %v", err)
}

func TestFilterGreaterOrEqualsWithIntIsTrue(t *testing.T) {
	expectedInt := 6
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test_column"], uint32(expectedInt))
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.GreaterOrEquals(row, expectedInt-2, "test_column")
	assert.True(t, retVal, "Return value should be true and it was false")
	assert.Nilf(t, err, "Filter GreaterOrEquals should not have thrown error. Error was: %v", err)

	retVal, err = filter.GreaterOrEquals(row, expectedInt, "test_column")
	assert.True(t, retVal, "Return value equal should be true and it was false")
	assert.Nilf(t, err, "Filter GreaterOrEquals should not have thrown error. Error was: %v", err)
}

func TestFilterGreaterOrEqualsWithIntIsFalse(t *testing.T) {
	expectedInt := 6
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test_column"], uint32(expectedInt))
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.GreaterOrEquals(row, expectedInt+2, "test_column")

	assert.False(t, retVal, "Return value lower should be false and it was true")
	assert.Nilf(t, err, "Filter GreaterOrEquals should not have thrown error. Error was: %v", err)
}

func TestFilterLessOrEqualsWithIntIsTrue(t *testing.T) {
	expectedInt := 6
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test_column"], uint32(expectedInt))
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.LessOrEquals(row, expectedInt+2, "test_column")
	assert.True(t, retVal, "Return value should be true and it was false")
	assert.Nilf(t, err, "Filter LessOrEquals should not have thrown error. Error was: %v", err)

	retVal, err = filter.LessOrEquals(row, expectedInt, "test_column")

	assert.True(t, retVal, "Return value equal should be true and it was false")
	assert.Nilf(t, err, "Filter LessOrEquals should not have thrown error. Error was: %v", err)
}

func TestFilterLessOrEqualThanWithIntIsFalse(t *testing.T) {
	expectedInt := 6
	dynMap := make(map[string][]byte)
	dynMap["test_column"] = make([]byte, 4)
	binary.BigEndian.PutUint32(dynMap["test_column"], uint32(expectedInt))
	dynMap["test_col2"] = []byte("test_not_pass_string")
	filter := NewFilter()
	row := dataStructures.NewDynamicMap(dynMap)
	retVal, err := filter.LessOrEquals(row, expectedInt-2, "test_column")

	assert.False(t, retVal, "Return value lower than should be false and it was true")
	assert.Nilf(t, err, "Filter LessOrEquals should not have thrown error. Error was: %v", err)
}
