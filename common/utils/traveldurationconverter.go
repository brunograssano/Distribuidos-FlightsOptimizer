package utils

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

const TravelDurationPrefix = "P"
const TravelTimePrefix = "T"
const TravelDurationDays = "D"
const TravelDurationHour = "H"
const TravelDurationMinute = "M"
const minutesInHour = 60
const hoursInDay = 24

func handleOnlyMinutes(minutesStr string) (int, error) {
	minutesStr, found := strings.CutPrefix(minutesStr, TravelTimePrefix)
	if !found {
		log.Warnf("TravelDurationConversion | Prefix '%v' not found in str '%v'", TravelTimePrefix, minutesStr)
	}

	minutes, err := strconv.Atoi(minutesStr)
	if err != nil {
		log.Warnf("TravelDurationConversion | Error converting minutes duration, will be sent as zero")
		minutes = 0
	}
	return minutes, nil
}

func handleOnlyDays(daysString string) (int, error) {
	day, err := strconv.Atoi(daysString)
	if err != nil {
		return -1, fmt.Errorf("day conversion error: %v", err)
	}
	return day * hoursInDay * minutesInHour, nil
}

func handleHoursAndMinutes(hoursStr string, minutesStr string) (int, error) {
	hoursStr, _ = strings.CutPrefix(hoursStr, TravelTimePrefix)
	hour := 0
	var err error
	if hoursStr != "" {
		hour, err = strconv.Atoi(hoursStr)
		if err != nil {
			return -1, fmt.Errorf("hour conversion error: %v", err)
		}
	}

	minutesString, found := strings.CutSuffix(minutesStr, TravelDurationMinute)
	minutes := 0
	if found {
		minutes, err = strconv.Atoi(minutesString)
		if err != nil {
			log.Warnf("Error converting minutes duration, will be sent as zero")
		}
	}

	return hour*minutesInHour + minutes, nil
}

// ConvertTravelDurationToMinutesAsInt
/*  Converts from the format dateFormat:scaled to minutes
* PT1M,
* PT1H,
* P1DT,
* PT3H18M
 */
func ConvertTravelDurationToMinutesAsInt(durationStr string) (int, error) {
	durStr, _ := strings.CutPrefix(durationStr, TravelDurationPrefix)
	before, _, found := strings.Cut(durStr, TravelDurationDays)
	if found { // Has days P1DT
		return handleOnlyDays(before)
	}

	// Has no day part
	hours, minutes, found := strings.Cut(before, TravelDurationHour)
	if found { // Has hours and minutes
		return handleHoursAndMinutes(hours, minutes)
	}

	minutes, _, found = strings.Cut(before, TravelDurationMinute)
	if found { // Only minutes
		return handleOnlyMinutes(minutes)
	}

	log.Warnf("TravelDurationConversion | Unknown time format '%v'", durationStr)
	return 0, fmt.Errorf("unknown format: %v", durationStr)
}
