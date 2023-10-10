package utils

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/communication"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
)

const TravelDurationPrefix = "PT"
const TravelDurationHour = "H"
const TravelDurationMinute = "M"
const minutesInHour = 60

// CloseFileAndNotifyError Closes a file and logs an error if any
func CloseFileAndNotifyError(file io.Closer) {
	err := file.Close()
	if err != nil {
		log.Errorf("Utils.CloseFileAndNotifyError | action: closing_file | status: error | %v", err)
	}
}

// CloseSocketAndNotifyError Closes a socket and logs an error if any
func CloseSocketAndNotifyError(s communication.TCPSocketInterface) {
	err := s.Close()
	if err != nil {
		log.Errorf("Utils.CloseSocketAndNotifyError | action: closing_socket | status: error | %v", err)
	}
}

func CreateSignalListener() chan os.Signal {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	return sigs
}

func ConvertTravelDurationToMinutesAsInt(durStr string) (int, error) {
	durStr, _ = strings.CutPrefix(durStr, TravelDurationPrefix)
	durationParts := strings.Split(durStr, TravelDurationHour)
	duration := 0
	//PT3H18M
	if len(durationParts) == 2 {
		hourString := durationParts[0]
		hour, err := strconv.Atoi(hourString)
		if err != nil {
			return -1, fmt.Errorf("hour conversion error: %v", err)
		}
		minutesString := durationParts[1]
		minutesString, found := strings.CutSuffix(minutesString, TravelDurationMinute)
		minutes := 0
		if found {
			minutes, err = strconv.Atoi(minutesString)
			if err != nil {
				log.Warnf("Error converting minutes duration, will be sent as zero")
			}
		}

		duration = hour*minutesInHour + minutes
	}
	return duration, nil
}
