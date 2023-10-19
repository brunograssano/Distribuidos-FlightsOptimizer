package saver

import (
	"errors"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/stretchr/testify/assert"
	"testing"
)

type FileWriterMock struct {
	err   error
	lines int
}

func (f *FileWriterMock) WriteLine(line string) error {
	if f.err != nil {
		return f.err
	}
	f.lines++
	return nil
}

func (f *FileWriterMock) Close() error {
	return nil
}

func TestTheSaverShouldWriteAllLines(t *testing.T) {
	writer := &FileWriterMock{lines: 0, err: nil}
	saver := &SimpleSaver{}

	dynMap := make(map[string][]byte)
	dynMap["col1"] = []byte("Some data")
	dynMap["col2"] = []byte("More data")

	row := dataStructures.NewDynamicMap(dynMap)
	err := saver.writeRowsToFile([]*dataStructures.DynamicMap{row, row, row}, writer)
	assert.Nilf(t, err, "Should not have thrown an error: %v", err)
	assert.Equalf(t, 3, writer.lines, "Should have wrote 3 lines")
}

func TestTheSaverShouldReturnAnError(t *testing.T) {
	writer := &FileWriterMock{lines: 0, err: errors.New("IO Error")}
	saver := &SimpleSaver{}

	dynMap := make(map[string][]byte)
	dynMap["col1"] = []byte("Some data")
	dynMap["col2"] = []byte("More data")

	row := dataStructures.NewDynamicMap(dynMap)
	err := saver.writeRowsToFile([]*dataStructures.DynamicMap{row, row, row}, writer)
	assert.Errorf(t, err, "Should have thrown an error")
}
