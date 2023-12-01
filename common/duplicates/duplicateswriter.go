package duplicates

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"strings"
)

func (dh *DuplicatesHandler) GetCheckpointString() string {
	linesToWrite := strings.Builder{}
	for clientId, messagesWithRowId := range dh.lastMessagesSeen {
		//{clientId},{messageId}={rowId},{messageId}={rowId},{messageId}={rowId},{messageId}={rowId}\n
		linesToWrite.WriteString(fmt.Sprintf("%v", clientId))
		for messageId, rowId := range messagesWithRowId {
			linesToWrite.WriteString(fmt.Sprintf(",%v=%v", messageId, rowId))
		}
		linesToWrite.WriteString("\n")
	}
	log.Info("%v %v", dh.queueName, dh.lastMessagesSeen)
	return linesToWrite.String()
}
