package checkpointer

type Checkpointable interface {
	DoCheckpoint(chan error, int)
	RestoreCheckpoint(CheckpointType, int, chan error)
	HasPendingCheckpoints(int) bool
	Commit(int, chan error)
	Abort(int, chan error)
}

type CheckpointType uint8

const (
	Tmp CheckpointType = iota
	Curr
	Old
)
