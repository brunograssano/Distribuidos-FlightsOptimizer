package main

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
	"github.com/brunograssano/Distribuidos-TP1/common/dispatcher"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	queueProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	"github.com/brunograssano/Distribuidos-TP1/common/queuefactory"
	log "github.com/sirupsen/logrus"
)

type DispatcherEx4 struct {
	dispatchers []*dispatcher.JourneyDispatcher
	c           *DispatcherEx4Config
	qMiddleware *middleware.QueueMiddleware
}

func NewDispatcherEx4(dispatcherConfig *DispatcherEx4Config) *DispatcherEx4 {
	qMiddleware := middleware.NewQueueMiddleware(dispatcherConfig.RabbitAddress)
	simpleFactory := queuefactory.NewSimpleQueueFactory(qMiddleware)

	var dispatchers []*dispatcher.JourneyDispatcher
	log.Infof("DispatcherEx4 | Creating %v dispatchers...", dispatcherConfig.DispatchersCount)
	for idx := uint(0); idx < dispatcherConfig.DispatchersCount; idx++ {
		checkpointerHandler := checkpointer.NewCheckpointerHandler()
		exchangeFactory := queuefactory.NewDirectExchangeProducerSimpleConsQueueFactory(qMiddleware)
		inputQueue := simpleFactory.CreateConsumer(dispatcherConfig.InputQueueName)
		prodToInput := simpleFactory.CreateProducer(dispatcherConfig.InputQueueName)
		var outputQueues []queueProtocol.ProducerProtocolInterface
		for i := uint(0); i < dispatcherConfig.SaversCount; i++ {
			outputQueue := exchangeFactory.CreateProducer(dispatcherConfig.OutputExchangeName)
			outputQueues = append(outputQueues, outputQueue)
		}
		tmpDispatcher := dispatcher.NewJourneyDispatcher(
			idx,
			inputQueue,
			prodToInput,
			outputQueues,
			checkpointerHandler,
			dispatcherConfig.TotalEofNodes,
			fmt.Sprintf("%v-%v", dispatcherConfig.ID, idx),
		)
		dispatchers = append(dispatchers, tmpDispatcher)
		checkpointerHandler.RestoreCheckpoint()
	}
	return &DispatcherEx4{
		dispatchers: dispatchers,
		c:           dispatcherConfig,
		qMiddleware: qMiddleware,
	}
}

func (de4 *DispatcherEx4) StartDispatch() {
	log.Infof("DispatcherEx4 | Spawning %v dispatchers...", de4.c.DispatchersCount)
	for idx := uint(0); idx < de4.c.DispatchersCount; idx++ {
		go de4.dispatchers[idx].DispatchLoop()
	}
}

func (de4 *DispatcherEx4) Close() {
	de4.qMiddleware.Close()
}
