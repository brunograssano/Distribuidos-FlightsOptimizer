package controllers

import "github.com/asmarques/geodist"

const KmToMiles = 1 / 1.60934

// CalculateDistanceFrom Calculates the distance between two points [lat, lon] returning it in miles
func CalculateDistanceFrom(start [2]float32, end [2]float32) float64 {
	originPoint := geodist.Point{Lat: float64(start[0]), Long: float64(start[1])}
	destPoint := geodist.Point{Lat: float64(end[0]), Long: float64(end[1])}
	return geodist.HaversineDistance(originPoint, destPoint) * KmToMiles
}
