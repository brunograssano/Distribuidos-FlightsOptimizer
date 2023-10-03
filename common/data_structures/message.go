package data_structures

type Message struct {
	TypeMessage int
	DynMaps     []*DynamicMap
}

const Airports = 0
const EOFAirports = 1
const FlightRows = 2
const EOFFlightRows = 3
const GetResults = 4
const Later = 5
