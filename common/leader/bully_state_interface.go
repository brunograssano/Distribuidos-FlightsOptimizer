package leader

type BullyState interface {
	AmILeader() bool
	Close()
}
