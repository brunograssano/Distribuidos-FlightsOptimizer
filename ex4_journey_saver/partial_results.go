package main

type PartialResult struct {
	filesToRead []string
	totalPrice  float32
	quantities  int
}

func NewPartialResult() *PartialResult {
	return &PartialResult{filesToRead: []string{}, totalPrice: 0, quantities: 0}
}
