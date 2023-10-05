package main

import (
	"client/parsers"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

// skipHeader Reads a line to skip the header
func skipHeader(reader *filemanager.FileReader) {
	if reader.CanRead() {
		_ = reader.ReadLine()
	}
}

// SendFile Sends the airports data to the server
func SendFile(FileName string, batchSize uint, conn *protocol.SocketProtocolHandler, parser parsers.Parser) error {
	reader, err := filemanager.NewFileReader(FileName)
	if err != nil {
		return err
	}
	defer utils.CloseFileAndNotifyError(reader)

	rows := make([]*dataStructures.DynamicMap, 0, batchSize)
	addedToMsg := uint(0)

	skipHeader(reader)
	for reader.CanRead() {
		line := reader.ReadLine()
		if addedToMsg >= batchSize {
			msg := &dataStructures.Message{TypeMessage: parser.GetMsgType(), DynMaps: rows}
			err = conn.Write(msg)
			if err != nil {
				log.Errorf("%v", err)
				return err
			}
			addedToMsg = 0
			rows = make([]*dataStructures.DynamicMap, 0, batchSize)
		}
		dynMap, err := parser.LineToDynMap(line)
		if err != nil {
			log.Errorf("Skipping line: %v", err)
			continue
		}
		rows = append(rows, dynMap)
		addedToMsg++
	}
	err = reader.Err()
	if err != nil {
		log.Errorf("%v", err)
		return err
	}
	if addedToMsg > 0 {
		msg := &dataStructures.Message{TypeMessage: parser.GetMsgType(), DynMaps: rows}
		err = conn.Write(msg)
	}

	return conn.Write(&dataStructures.Message{TypeMessage: parser.GetEofMsgType()})
}
