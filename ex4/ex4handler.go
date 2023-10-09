package main

import (
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/dispatcher"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	log "github.com/sirupsen/logrus"
)

// Ex4Handler Struct containing the services of the exercise 4
type Ex4Handler struct {
	c                 *Ex4Config
	journeyDispatcher *dispatcher.JourneyDispatcher
	savers            []*JourneySaver
	accumulator       *AvgCalculator
	qMiddleware       *middleware.QueueMiddleware
	journeySink       *JourneySink
	channels          []chan *dataStructures.Message
}

// NewEx4Handler Creates a new exercise 4 handler
func NewEx4Handler(c *Ex4Config) *Ex4Handler {
	var channels []chan *dataStructures.Message
	qMiddleware := middleware.NewQueueMiddleware(c.RabbitAddress)
	// We create the input queue to the EX4 service
	inputQueue := protocol.NewConsumerQueueProtocolHandler(qMiddleware.CreateConsumer(c.InputQueueName, true))
	// We create the output queue to the saver of the end results
	outputQueue := protocol.NewProducerQueueProtocolHandler(qMiddleware.CreateProducer(c.OutputQueueName, true))

	// Creation of the queue to the average calculator
	accumChannel := make(chan *dataStructures.Message)
	toAccumulatorChannelProducer := protocol.NewProducerChannel(accumChannel)
	toAccumulatorChannelConsumer := protocol.NewConsumerChannel(accumChannel)
	// We append the channels so that we can close all of them later
	channels = append(channels, accumChannel)

	// Creation of the JourneySink, it will redirect and handle EOF to saver
	journeySinkChannel := make(chan *dataStructures.Message)
	toJourneySinkChannelProducer := protocol.NewProducerChannel(journeySinkChannel)
	channels = append(channels, journeySinkChannel)

	// Creation of the JourneySavers, they handle the prices per journey
	var internalSavers []*JourneySaver
	var toInternalSaversChannels []protocol.ProducerProtocolInterface
	log.Infof("Ex4Handler | Creating %v journey savers...", int(c.InternalSaversCount))
	for i := 0; i < int(c.InternalSaversCount); i++ {
		internalServerChannel := make(chan *dataStructures.Message)
		channels = append(channels, internalServerChannel)
		internalSavers = append(internalSavers, NewJourneySaver(
			protocol.NewConsumerChannel(internalServerChannel),
			toAccumulatorChannelProducer,
			toJourneySinkChannelProducer,
		))
		toInternalSaversChannels = append(toInternalSaversChannels, protocol.NewProducerChannel(internalServerChannel))
		log.Infof("Ex4Handler | Created Saver #%v correctly...", i)
	}

	// Creation of the dispatcher to the JourneySavers
	log.Infof("Ex4Handler | Creating dispatcher...")
	jd := dispatcher.NewJourneyDispatcher(inputQueue, toInternalSaversChannels)

	return &Ex4Handler{
		c:                 c,
		journeyDispatcher: jd,
		accumulator:       NewAvgCalculator(toInternalSaversChannels, toAccumulatorChannelConsumer),
		qMiddleware:       qMiddleware,
		channels:          channels,
		savers:            internalSavers,
		journeySink:       NewJourneySink(protocol.NewConsumerChannel(journeySinkChannel), outputQueue, c.InternalSaversCount),
	}
}

// StartHandler Starts the exercise 4 services as goroutines
func (ex4h *Ex4Handler) StartHandler() {
	log.Debugf("Number of savers is: %v", len(ex4h.savers))
	for idx, saver := range ex4h.savers {
		log.Infof("Spawning saver #%v", idx+1)
		go saver.SavePricesForJourneys()
	}
	log.Infof("Ex4Handler | Spawning General Accumulator")
	go ex4h.accumulator.CalculateAvgLoop()
	log.Infof("Ex4Handler | Spawning Journey Sink")
	go ex4h.journeySink.HandleJourneys()

	log.Infof("Ex4Handler | Spawning Dispatcher")
	go ex4h.journeyDispatcher.DispatchLoop()
}

// Close Closes the handler of the exercise 4
func (ex4h *Ex4Handler) Close() {
	ex4h.qMiddleware.Close()
	for idx, channel := range ex4h.channels {
		log.Infof("Closing channel #%v", idx)
		close(channel)
	}
}
