package main

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

const partialSumFields = 3

type PartialSum struct {
	sumOfPrices float32
	sumOfRows   int
	numOfSavers int
}

func (ps *PartialSum) Serialize() string {
	return fmt.Sprintf("%v,%v,%v", ps.sumOfPrices, ps.sumOfRows, ps.numOfSavers)
}

func DeserializePartialSum(partialSumString string) (*PartialSum, error) {
	partsOfPartialSum := strings.Split(partialSumString, utils.CommaSeparator)
	if len(partsOfPartialSum) != partialSumFields {
		return nil, fmt.Errorf("got different format for partial sum %v", partsOfPartialSum)
	}
	sumOfPrices, err := strconv.ParseFloat(partsOfPartialSum[0], 32)
	if err != nil {
		log.Errorf("PartialSum | Error converting sum of prices to float32 | %v", err)
		return nil, err
	}
	sumOfRows, err := strconv.Atoi(partsOfPartialSum[1])
	if err != nil {
		log.Errorf("PartialSum | Error converting rowSum to int | %v", err)
		return nil, err
	}
	numOfSavers, err := strconv.Atoi(partsOfPartialSum[2])
	if err != nil {
		log.Errorf("PartialSum | Error converting saversNum to int | %v", err)
		return nil, err
	}
	return &PartialSum{float32(sumOfPrices), sumOfRows, numOfSavers}, nil
}
