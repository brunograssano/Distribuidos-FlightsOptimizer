package main

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/dispatcher"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	log "github.com/sirupsen/logrus"
)

type Ex4Handler struct {
	c                 *Ex4Config
	journeyDispatcher *dispatcher.JourneyDispatcher
	savers            []*JourneySaver
	accumulator       *AvgCalculator
	qMiddleware       *middleware.QueueMiddleware
	channels          []chan *dataStructures.Message
}

func NewEx4Handler(c *Ex4Config) *Ex4Handler {
	var channels []chan *dataStructures.Message
	qMiddleware := middleware.NewQueueMiddleware(c.RabbitAddress)
	inputQueue := protocol.NewConsumerQueueProtocolHandler(qMiddleware.CreateConsumer(c.InputQueueName, true))
	outputQueue := protocol.NewProducerQueueProtocolHandler(qMiddleware.CreateProducer(c.OutputQueueName, true))

	accumChannel := make(chan *dataStructures.Message)
	channels = append(channels, accumChannel)
	toAccumulatorChannelProducer := protocol.NewProducerChannel(accumChannel)
	toAccumulatorChannelConsumer := protocol.NewConsumerChannel(accumChannel)

	var internalSavers []*JourneySaver
	var toInternalSaversChannels []protocol.ProducerProtocolInterface
	log.Infof("Creating %v savers...", int(c.InternalSaversCount))
	for i := 0; i < int(c.InternalSaversCount); i++ {
		internalServerChannel := make(chan *dataStructures.Message)
		channels = append(channels, internalServerChannel)
		internalSavers = append(internalSavers, NewJourneySaver(
			protocol.NewConsumerChannel(internalServerChannel),
			toAccumulatorChannelProducer,
			outputQueue,
		))
		toInternalSaversChannels = append(toInternalSaversChannels, protocol.NewProducerChannel(internalServerChannel))
		log.Debugf("Spawned Saver #%v correctly...", i)
	}
	log.Debugf("Spawning dispatcher...")
	jd := dispatcher.NewJourneyDispatcher(inputQueue, toInternalSaversChannels)

	return &Ex4Handler{
		c:                 c,
		journeyDispatcher: jd,
		accumulator:       NewAvgCalculator(toInternalSaversChannels, toAccumulatorChannelConsumer),
		qMiddleware:       qMiddleware,
		channels:          channels,
	}
}

func (ex4h *Ex4Handler) StartHandler() {
	log.Debugf("Number of savers is: %v", len(ex4h.savers))
	for idx, saver := range ex4h.savers {
		log.Infof("Spawning saver #%v", idx+1)
		go saver.SavePricesForJourneys()
	}
	log.Infof("Spawning General Accumulator")
	go ex4h.accumulator.CalculateAvgLoop()
}

func (ex4h *Ex4Handler) Close() {
	ex4h.qMiddleware.Close()
	for idx, channel := range ex4h.channels {
		log.Infof("Closing channel #%v", idx)
		close(channel)
	}
}
