package main

import (
	"bytes"
	log "github.com/sirupsen/logrus"
	"os/exec"
	"time"
)

func main() {
	for {
		cmd := exec.Command("docker", "ps")
		var outb, errb bytes.Buffer
		cmd.Stdout = &outb
		cmd.Stderr = &errb
		err := cmd.Run()
		if err != nil {
			log.Errorf("Error running docker ps: %v", err)
		}
		log.Infof("out: %v || err: %v", outb.String(), errb.String())
		time.Sleep(1 * time.Second)
	}
}
