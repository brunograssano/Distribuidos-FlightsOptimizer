package queues

func (q *ConsumerQueueProtocolHandler) DoCheckpoint(errors chan error, id int, chkId int) {
	q.duplicatesHandler.DoCheckpoint(errors, id, chkId)
}

func (q *ConsumerQueueProtocolHandler) Commit(id int, response chan error) {
	q.duplicatesHandler.Commit(id, response)
}

func (q *ConsumerQueueProtocolHandler) Abort(id int, response chan error) {
	q.duplicatesHandler.Abort(id, response)
}

func (q *ConsumerQueueProtocolHandler) RestoreCheckpoint(checkpointToRestore int, id int, result chan error) {
	q.duplicatesHandler.RestoreCheckpoint(checkpointToRestore, id, result)
}

func (q *ConsumerQueueProtocolHandler) GetCheckpointVersions(id int) [2]int {
	return q.duplicatesHandler.GetCheckpointVersions(id)
}
