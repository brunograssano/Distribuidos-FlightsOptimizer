package duplicates

import (
	"fmt"
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
	return linesToWrite.String()
}
