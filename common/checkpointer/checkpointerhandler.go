package checkpointer

import (
	"fmt"
	log "github.com/sirupsen/logrus"
)

type CheckpointerHandler struct {
	checkpointers []Checkpointable
}

func NewCheckpointerHandler(checkpointers []Checkpointable) *CheckpointerHandler {
	return &CheckpointerHandler{
		checkpointers,
	}
}

func (c *CheckpointerHandler) DoCheckpoint() error {
	responses := make(chan error, len(c.checkpointers))
	log.Debugf("CheckpointerHandler | Initializing Checkpointing...")
	for _, checkpointable := range c.checkpointers {
		go checkpointable.DoCheckpoint(responses)
	}
	doCommit := true
	log.Debugf("CheckpointerHandller | Checking for checkpointers responses...")
	for i := 0; i < len(c.checkpointers); i++ {
		err := <-responses
		if err != nil {
			log.Errorf("CheckpointerHandler | Error trying to checkpoint | %v", err)
			doCommit = false
		}
	}
	if doCommit {
		log.Debugf("CheckpointerHandler | Commiting Checkpoint...")
		for _, checkpointable := range c.checkpointers {
			go checkpointable.Commit()
		}
		return nil
	}

	log.Debugf("CheckpointerHandler | Aborting Checkpoint...")
	for _, checkpointable := range c.checkpointers {
		go checkpointable.Abort()
	}
	return fmt.Errorf("error trying to do checkpoint, operation was aborted")
}
