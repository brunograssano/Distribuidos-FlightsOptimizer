package getters

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/serializer"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
)

func GetExerciseMessageWithRow(uuid string, exercise int, row int) *dataStructures.Message {
	msgToReconnectWith := dataStructures.NewGetResultsMessage(uuid)
	mapForDM := make(map[string][]byte)
	mapForDM[utils.Exercise] = serializer.SerializeUint(uint32(exercise))
	mapForDM[utils.NumberOfRow] = serializer.SerializeUint(uint32(row))
	msgToReconnectWith.DynMaps = []*dataStructures.DynamicMap{dataStructures.NewDynamicMap(mapForDM)}
	return msgToReconnectWith
}
