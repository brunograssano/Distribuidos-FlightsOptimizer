package checkpointer

type Checkpointable interface {
	DoCheckpoint(chan error)
	RestoreCheckpoint(fileType CheckpointType)
	HasPendingCheckpoints() bool
	Commit()
	Abort()
}

type CheckpointType uint8

const (
	Tmp CheckpointType = iota
	Curr
	Old
)
