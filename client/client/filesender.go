package client

import (
	"client/client/parsers"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	socketsProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/sockets"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

// SendFile Sends a file data through a socket
func SendFile(FileName string, conf *ClientConfig, conn *socketsProtocol.SocketProtocolHandler, parser parsers.Parser) error {
	reader, err := filemanager.NewFileReader(FileName)
	if err != nil {
		return err
	}
	defer utils.CloseFileAndNotifyError(reader.FileManager)

	rows := make([]*dataStructures.DynamicMap, 0, conf.Batch)
	addedToMsg := uint(0)

	messageId := uint(0)
	filemanager.SkipHeader(reader)
	for reader.CanRead() {
		line := reader.ReadLine()
		if addedToMsg >= conf.Batch {
			msg := dataStructures.NewCompleteMessage(parser.GetMsgType(), rows, conf.Uuid, messageId)
			messageId++
			err = conn.Write(msg)
			if err != nil {
				log.Errorf("FileSend | Error trying to send file | %v", err)
				return err
			}
			addedToMsg = 0
			rows = make([]*dataStructures.DynamicMap, 0, conf.Batch)
		}
		dynMap, err := parser.LineToDynMap(line)
		if err != nil {
			log.Errorf("FileSend | %v | Skipping line", err)
			continue
		}
		rows = append(rows, dynMap)
		addedToMsg++
	}
	err = reader.Err()
	if err != nil {
		log.Errorf("FileSend | %v", err)
		return err
	}
	if addedToMsg > 0 {
		msg := dataStructures.NewCompleteMessage(parser.GetMsgType(), rows, conf.Uuid, messageId)
		messageId++
		// TODO verificar error
		err = conn.Write(msg)
	}

	return conn.Write(dataStructures.NewCompleteMessage(parser.GetEofMsgType(), []*dataStructures.DynamicMap{}, conf.Uuid, messageId))
}
