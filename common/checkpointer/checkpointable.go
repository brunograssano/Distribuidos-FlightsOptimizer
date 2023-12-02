package checkpointer

type Checkpointable interface {
	DoCheckpoint(chan error, int, int)
	RestoreCheckpoint(int, int, chan error)
	GetCheckpointVersions(int) [2]int
	Commit(int, chan error)
	Abort(int, chan error)
}
