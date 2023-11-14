package main

import (
	"fmt"
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
		inputQueue := simpleFactory.CreateConsumer(dispatcherConfig.InputQueueName)
		prodToInput := simpleFactory.CreateProducer(dispatcherConfig.InputQueueName)
		var outputQueues []queueProtocol.ProducerProtocolInterface
		for i := uint(0); i < dispatcherConfig.SaversCount; i++ {
			outputQueue := queueProtocol.NewProducerQueueProtocolHandler(
				qMiddleware.CreateExchangeProducer(dispatcherConfig.OutputExchangeName, fmt.Sprintf("%v", i), "direct", true),
			)
			outputQueues = append(outputQueues, outputQueue)
		}
		dispatchers = append(dispatchers, dispatcher.NewJourneyDispatcher(inputQueue, prodToInput, outputQueues))
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
