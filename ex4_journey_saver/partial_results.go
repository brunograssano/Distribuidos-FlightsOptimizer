package main

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

type PartialResult struct {
	filesToRead []string
	totalPrice  float32
	quantities  int
}

func NewPartialResult() *PartialResult {
	return &PartialResult{filesToRead: []string{}, totalPrice: 0, quantities: 0}
}

func (r *PartialResult) Serialize() string {
	//ftr1,ftr2,ftr3,...;totalPrice;quantities
	var builder strings.Builder
	builder.WriteString(strings.Join(r.filesToRead, utils.CommaSeparator))
	builder.WriteString(utils.DotCommaSeparator)
	builder.WriteString(fmt.Sprintf("%v", r.totalPrice))
	builder.WriteString(utils.DotCommaSeparator)
	builder.WriteString(fmt.Sprintf("%v", r.quantities))
	return builder.String()
}

func DeserializePartialResultFromStr(partialResStr string) (*PartialResult, error) {
	filesToReadTotalPriceAndQuantities := strings.Split(partialResStr, utils.DotCommaSeparator)
	if len(filesToReadTotalPriceAndQuantities) != 3 {
		return nil, fmt.Errorf("wrong partial result format %v", filesToReadTotalPriceAndQuantities)
	}
	filesToRead := strings.Split(filesToReadTotalPriceAndQuantities[0], utils.CommaSeparator)
	totalPrice, err := strconv.ParseFloat(filesToReadTotalPriceAndQuantities[1], 32)
	if err != nil {
		log.Errorf("PartialResult | Error trying to convert total price into float32 | %v", err)
		return nil, err
	}
	quantities, err := strconv.Atoi(filesToReadTotalPriceAndQuantities[2])
	if err != nil {
		log.Errorf("PartialResult | Error trying to convert quantities into int | %v", err)
		return nil, err
	}
	return &PartialResult{filesToRead, float32(totalPrice), quantities}, nil
}
