package checkpointer

import (
	"fmt"
	log "github.com/sirupsen/logrus"
)

type CheckpointerHandler struct {
	checkpointersById map[int][]Checkpointable
}

func NewCheckpointerHandler() *CheckpointerHandler {
	return &CheckpointerHandler{
		checkpointersById: make(map[int][]Checkpointable),
	}
}

func (c *CheckpointerHandler) AddCheckpointable(checkpointable Checkpointable, id int) {
	_, exists := c.checkpointersById[id]
	if !exists {
		c.checkpointersById[id] = []Checkpointable{}
	}
	c.checkpointersById[id] = append(c.checkpointersById[id], checkpointable)
}

func (c *CheckpointerHandler) DoCheckpoint(idCheckpointer int) error {
	checkpointers := c.checkpointersById[idCheckpointer]
	responses := make(chan error, len(checkpointers))
	log.Debugf("CheckpointerHandler | Initializing Checkpointing...")
	for _, checkpointable := range checkpointers {
		go checkpointable.DoCheckpoint(responses, idCheckpointer)
	}
	doCommit := true
	log.Debugf("CheckpointerHandller | Checking for checkpointersById responses...")
	for i := 0; i < len(checkpointers); i++ {
		err := <-responses
		if err != nil {
			log.Errorf("CheckpointerHandler | Error trying to checkpoint | %v", err)
			doCommit = false
		}
	}
	if doCommit {
		log.Debugf("CheckpointerHandler | Commiting Checkpoint...")
		for _, checkpointable := range checkpointers {
			go checkpointable.Commit(idCheckpointer)
		}
		return nil
	}

	log.Debugf("CheckpointerHandler | Aborting Checkpoint...")
	for _, checkpointable := range checkpointers {
		go checkpointable.Abort(idCheckpointer)
	}
	return fmt.Errorf("error trying to do checkpoint, operation was aborted")
}

func (c *CheckpointerHandler) RestoreCheckpoint() {
	checkpointType := Curr
	for _, checkpointablesForProcess := range c.checkpointersById {
		for idx, checkpointable := range checkpointablesForProcess {
			if checkpointable.HasPendingCheckpoints(idx) {
				checkpointType = Old
			}
		}
	}
	for _, checkpointablesForProcess := range c.checkpointersById {
		for idx, checkpointable := range checkpointablesForProcess {
			go checkpointable.RestoreCheckpoint(checkpointType, idx)
		}
	}
}
