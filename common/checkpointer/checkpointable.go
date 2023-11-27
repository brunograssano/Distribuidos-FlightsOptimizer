package checkpointer

type Checkpointable interface {
	DoCheckpoint(chan error, int)
	RestoreCheckpoint(CheckpointType, int)
	HasPendingCheckpoints(int) bool
	Commit(int)
	Abort(int)
}

type CheckpointType uint8

const (
	Tmp CheckpointType = iota
	Curr
	Old
)
