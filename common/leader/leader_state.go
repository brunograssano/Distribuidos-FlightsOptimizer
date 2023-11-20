package leader

type LeaderState struct{}

func NewLeaderState() *LeaderState {
	return &LeaderState{}
}

func (ls *LeaderState) AmILeader() bool {
	return true
}

func (ls *LeaderState) Close() {
	return
}
