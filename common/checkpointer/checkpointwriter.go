package checkpointer

type CheckpointWriter interface {
	GetCheckpointString() string
}
