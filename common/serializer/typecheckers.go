package serializer

import (
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	"slices"
)

func isFloatColumn(key string) bool {
	floatColumnKeys := []string{
		utils.Latitude,
		utils.Longitude,
		utils.TotalFare,
		utils.TotalTravelDistance,
		utils.DirectDistance,
		utils.Max,
		utils.Avg,
	}
	return slices.Contains(floatColumnKeys, key)
}

func isIntColumn(key string) bool {
	intColumnKeys := []string{
		utils.TotalStopovers,
		utils.ConvertedTravelDuration,
	}
	return slices.Contains(intColumnKeys, key)
}
