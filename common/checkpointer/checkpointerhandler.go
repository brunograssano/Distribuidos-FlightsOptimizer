package checkpointer

import (
	"fmt"
	log "github.com/sirupsen/logrus"
)

type CheckpointerHandler struct {
	checkpointersById map[int][]Checkpointable
	chkVersion        map[int]int
}

func NewCheckpointerHandler() *CheckpointerHandler {
	return &CheckpointerHandler{
		checkpointersById: make(map[int][]Checkpointable),
		chkVersion:        make(map[int]int),
	}
}

func (c *CheckpointerHandler) AddCheckpointable(checkpointable Checkpointable, id int) {
	_, exists := c.checkpointersById[id]
	if !exists {
		c.checkpointersById[id] = []Checkpointable{}
		c.chkVersion[id] = 0
	}
	c.checkpointersById[id] = append(c.checkpointersById[id], checkpointable)
}

func (c *CheckpointerHandler) DoCheckpoint(idCheckpointer int) error {
	checkpointers := c.checkpointersById[idCheckpointer]
	responses := make(chan error, len(checkpointers))
	log.Debugf("CheckpointerHandler | Initializing Checkpointing for %v...", idCheckpointer)
	for _, checkpointable := range checkpointers {
		go checkpointable.DoCheckpoint(responses, idCheckpointer, c.chkVersion[idCheckpointer])
	}
	doCommit := true
	log.Debugf("CheckpointerHandller | Checking for checkpointersById responses for %v...", idCheckpointer)
	for i := 0; i < len(checkpointers); i++ {
		err := <-responses
		if err != nil {
			log.Errorf("CheckpointerHandler | Error trying to checkpoint | %v", err)
			doCommit = false
		}
	}
	if doCommit {
		c.chkVersion[idCheckpointer]++
		log.Debugf("CheckpointerHandler | Commiting Checkpoint for %v", idCheckpointer)
		for _, checkpointable := range checkpointers {
			go checkpointable.Commit(idCheckpointer, responses)
		}
		waitForResponses(len(checkpointers), responses)
		log.Debugf("CheckpointerHandler | Commited Checkpoint for %v", idCheckpointer)
		return nil
	}

	log.Debugf("CheckpointerHandler | Aborting Checkpoint for %v", idCheckpointer)
	for _, checkpointable := range checkpointers {
		go checkpointable.Abort(idCheckpointer, responses)
	}
	waitForResponses(len(checkpointers), responses)
	log.Debugf("CheckpointerHandler | Aborted Checkpoint for %v", idCheckpointer)
	return fmt.Errorf("error trying to do checkpoint, operation was aborted")
}

func waitForResponses(waitForCheckpointables int, responses chan error) {
	for i := 0; i < waitForCheckpointables; i++ {
		<-responses
	}
}

func (c *CheckpointerHandler) RestoreCheckpoint() {
	totalCheckpointables := 0
	for _, checkpointablesForProcess := range c.checkpointersById {
		totalCheckpointables += len(checkpointablesForProcess)
	}
	responses := make(chan error, totalCheckpointables)
	for id, checkpointablesForProcess := range c.checkpointersById {
		versionToRestore := getVersionToRestore(checkpointablesForProcess, id)
		c.chkVersion[id] = versionToRestore + 1
		if versionToRestore == -1 {
			log.Infof("CheckpointerHandler | No valid checkpoint to restore for %v", id)
			totalCheckpointables -= len(checkpointablesForProcess)
			continue
		}
		log.Infof("CheckpointerHandler | ID: %v | Using checkpoint %v", id, versionToRestore)
		for _, checkpointable := range checkpointablesForProcess {
			go checkpointable.RestoreCheckpoint(versionToRestore, id, responses)
		}

	}
	for i := 0; i < totalCheckpointables; i++ {
		<-responses
	}
}

func getVersionsAvailable(checkpointablesForProcess []Checkpointable, id int) map[int]int {
	versionsAvailable := make(map[int]int)
	for _, checkpointable := range checkpointablesForProcess {
		versions := checkpointable.GetCheckpointVersions(id)
		for _, version := range versions {
			_, exists := versionsAvailable[version]
			if !exists {
				versionsAvailable[version] = 0
			}
			versionsAvailable[version]++
		}
	}
	return versionsAvailable
}

func getVersionToRestore(checkpointablesForProcess []Checkpointable, id int) int {
	versionsAvailable := getVersionsAvailable(checkpointablesForProcess, id)
	versionToRestore := -1
	for version, availability := range versionsAvailable {
		if version > versionToRestore && availability == len(checkpointablesForProcess) {
			versionToRestore = version
		}
	}
	return versionToRestore
}
