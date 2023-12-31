package client

import (
	"client/client/parsers"
	"fmt"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	socketsProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/sockets"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"time"
)

const minimumSleepExpBackoff = 2
const backoffPower = 2
const maximumSleepExpBackoff = 16

func resendMessageAndPreviousOne(previousMessage *dataStructures.Message, msg *dataStructures.Message, conn *socketsProtocol.SocketProtocolHandler) error {
	if previousMessage != nil {
		err := conn.Write(previousMessage)
		if err != nil {
			return err
		}
	}
	return conn.Write(msg)
}

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

	var previousMessage *dataStructures.Message
	filemanager.SkipHeader(reader)
	for reader.CanRead() {
		line := reader.ReadLine()
		if addedToMsg >= conf.Batch {
			msg := dataStructures.NewCompleteMessage(parser.GetMsgType(), rows, conf.Uuid, messageId)
			messageId++
			sendWithReconnection(conn, msg, previousMessage)
			if err != nil {

			}
			previousMessage = msg
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
		sendWithReconnection(conn, msg, previousMessage)
		previousMessage = msg
	}
	return sendEOFAndWaitForACK(conn, parser, conf, messageId, previousMessage)
}

func sendEOFAndWaitForACK(conn *socketsProtocol.SocketProtocolHandler, parser parsers.Parser, conf *ClientConfig, messageId uint, previousMessage *dataStructures.Message) error {
	eofMsg := dataStructures.NewCompleteMessage(parser.GetEofMsgType(), []*dataStructures.DynamicMap{}, conf.Uuid, messageId)
	sendWithReconnection(conn, eofMsg, previousMessage)
	msg, err := conn.Read()
	if err != nil {
		log.Errorf("FileSend | Error sending EOF to server | Retrying...")
		err = sendEOFAndWaitForACK(conn, parser, conf, messageId, previousMessage)
		if err != nil {
			return err
		}
		return nil
	}
	if msg.TypeMessage == dataStructures.EofAck {
		log.Infof("FileSend | Got ACK for the sent EOF | Finishing File Send Loop...")
		return nil
	}
	return fmt.Errorf("got unexpected type of message")
}

func sendWithReconnection(conn *socketsProtocol.SocketProtocolHandler, msg *dataStructures.Message, previousMessage *dataStructures.Message) {
	err := conn.Write(msg)
	if err != nil {
		log.Errorf("FileSend | Error trying to send file | %v | Trying to reconnect...", err)
		currSleep := minimumSleepExpBackoff
		for {
			err = conn.Reconnect()
			if err == nil {
				log.Infof("FileSend | Reconnected with server | Resending Previous and Current Message...")
				err = resendMessageAndPreviousOne(previousMessage, msg, conn)
				if err == nil {
					break
				}
			}
			time.Sleep(time.Duration(currSleep) * time.Second)
			currSleep = currSleep * backoffPower
			if currSleep > maximumSleepExpBackoff {
				currSleep = maximumSleepExpBackoff
			}
		}
	}
}
