package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestShouldParseTheStringWithHoursAndMinutes(t *testing.T) {
	duration, err := ConvertTravelDurationToMinutesAsInt("PT3H18M")

	assert.Nilf(t, err, "Got an error when parsing duration %v", err)
	assert.Equalf(t, 3*60+18, duration, "Got different duration %v", duration)
}

func TestShouldParseTheStringWithOnlyHours(t *testing.T) {
	duration, err := ConvertTravelDurationToMinutesAsInt("PT3H")

	assert.Nilf(t, err, "Got an error when parsing duration %v", err)
	assert.Equalf(t, 3*60, duration, "Got different duration %v", duration)
}

func TestShouldParseTheStringWithOnlyMinutes(t *testing.T) {
	duration, err := ConvertTravelDurationToMinutesAsInt("PT40M")

	assert.Nilf(t, err, "Got an error when parsing duration %v", err)
	assert.Equalf(t, 40, duration, "Got different duration %v", duration)
}

func TestShouldParseTheStringWithOnlyDays(t *testing.T) {
	duration, err := ConvertTravelDurationToMinutesAsInt("P1DT")

	assert.Nilf(t, err, "Got an error when parsing duration %v", err)
	assert.Equalf(t, 1*24*60, duration, "Got different duration %v", duration)
}

func TestShouldNotUnderstandTheFormat(t *testing.T) {
	duration, err := ConvertTravelDurationToMinutesAsInt("P1WT")

	assert.Errorf(t, err, "Did not got an error when parsing duration: %v", duration)
}
