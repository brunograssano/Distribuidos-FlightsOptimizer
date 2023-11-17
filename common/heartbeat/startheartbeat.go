package heartbeat

const timePerHeartbeat = uint32(5)

func StartHeartbeat(HealthCheckers []string, Name string) chan bool {
	endSigHB := make(chan bool, 1)
	go heartBeatLoop(HealthCheckers, Name, timePerHeartbeat, endSigHB)
	return endSigHB
}
