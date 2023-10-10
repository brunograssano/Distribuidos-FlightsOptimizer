package utils

import (
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	"testing"
)

func TestShouldParseTheStringWithHoursAndMinutes(t *testing.T) {
	duration, err := utils.ConvertTravelDurationToMinutesAsInt("PT3H18M")
	if err != nil {
		t.Errorf("Got an error when parsing duration %v", err)
	}
	if duration != 3*60+18 {
		t.Errorf("Got different duration %v", duration)
	}
}

func TestShouldParseTheStringWithOnlyHours(t *testing.T) {
	duration, err := utils.ConvertTravelDurationToMinutesAsInt("PT3H")
	if err != nil {
		t.Errorf("Got an error when parsing duration %v", err)
	}
	if duration != 3*60 {
		t.Errorf("Got different duration %v", duration)
	}
}

func TestShouldParseTheStringWithOnlyMinutes(t *testing.T) {
	duration, err := utils.ConvertTravelDurationToMinutesAsInt("PT40M")
	if err != nil {
		t.Errorf("Got an error when parsing duration %v", err)
	}
	if duration != 40 {
		t.Errorf("Got different duration %v", duration)
	}
}

func TestShouldParseTheStringWithOnlyDays(t *testing.T) {
	duration, err := utils.ConvertTravelDurationToMinutesAsInt("P1DT")
	if err != nil {
		t.Errorf("Got an error when parsing duration %v", err)
	}
	if duration != 1*24*60 {
		t.Errorf("Got different duration %v", duration)
	}
}

func TestShouldNotUnderstandTheFormat(t *testing.T) {
	duration, err := utils.ConvertTravelDurationToMinutesAsInt("P1WT")
	if err == nil {
		t.Errorf("Did not got an error when parsing duration: %v", duration)
	}
}
