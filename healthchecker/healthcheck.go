package main

import (
	"bytes"
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	socketsProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/sockets"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"os/exec"
	"time"
)

type HealthChecker struct {
	server             *communication.PassiveTCPSocket
	timesLastHeartbeat map[string]time.Time
	config             *Config
	endSignal          chan bool
}

func NewHealthChecker(healthCheckerConfig *Config) *HealthChecker {
	server, err := communication.NewPassiveTCPSocket(healthCheckerConfig.Address)
	if err != nil {
		log.Fatalf("HealthChecker | Error instantiating server | %v", err)
	}
	return &HealthChecker{
		server:             server,
		timesLastHeartbeat: make(map[string]time.Time),
		config:             healthCheckerConfig,
		endSignal:          make(chan bool, 1),
	}
}

func (h *HealthChecker) HandleHeartBeats() {
	go h.acceptIncomingConnections()
	for {
		timeout := time.After(time.Duration(h.config.CheckTime) * time.Second)
		select {
		case <-h.endSignal:
			log.Infof("HealthChecker Loop | Closing health checker goroutine")
		case <-timeout:
			log.Debugf("HealthChecker Loop | Now Checking If Someone Needs Restart...")
			h.checkRestarts()
		}
	}
}

func (h *HealthChecker) checkRestarts() {
	timeToCheckWith := time.Now()
	restartTimeParsed := time.Duration(h.config.RestartTime) * time.Second
	for serviceName, timestamp := range h.timesLastHeartbeat {
		timeDiff := timeToCheckWith.Sub(timestamp)
		if timeDiff > restartTimeParsed {
			log.Infof("HealthChecker | Detected that %v is not heartbeating | Restarting service...", serviceName)
			go h.restart(serviceName)
		}

	}
}

func (h *HealthChecker) restart(name string) {
	cmd := exec.Command("docker", "start", name)
	var outCommand, errCommand bytes.Buffer
	cmd.Stdout = &outCommand
	cmd.Stderr = &errCommand
	err := cmd.Run()
	if err != nil {
		log.Errorf("HealthChecker | Error running docker start %v | %v", name, err)
	} else {
		h.timesLastHeartbeat[name] = time.Now()
	}
	log.Debugf("HealthChecker | out: %v | err: %v", outCommand.String(), errCommand.String())
}

func (h *HealthChecker) acceptIncomingConnections() {
	for {
		conn, err := h.server.Accept()
		if err != nil {
			log.Errorf("Healthchecker | Error accepting connection, finishing loop | Err: %v", err)
			return
		}
		go h.handleAcceptedConnection(conn)
	}
}

func (h *HealthChecker) handleAcceptedConnection(conn *communication.TCPSocket) {
	sph := socketsProtocol.NewSocketProtocolHandler(conn)
	defer sph.Close()
	msg, err := sph.Read()
	if err != nil {
		log.Errorf("Healthchecker | Error receiving from connection | Err: %v", err)
		return
	}
	if msg.TypeMessage != dataStructures.HeartBeat {
		log.Warnf("Healthchecker | Received unknown message type, skipping... | MsgType: %v", msg.TypeMessage)
		return
	}
	serviceName, err := msg.DynMaps[0].GetAsString("name")
	if err != nil {
		log.Errorf("Healthchecker | Missing name for service | Err: %v", err)
		return
	}
	h.timesLastHeartbeat[serviceName] = time.Now()
}

func (h *HealthChecker) Close() {
	utils.CloseSocketAndNotifyError(h.server)
	h.endSignal <- true
}
