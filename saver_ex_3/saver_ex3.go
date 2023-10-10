package main

import (
	"fmt"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/dispatcher"
	"github.com/brunograssano/Distribuidos-TP1/common/getters"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	log "github.com/sirupsen/logrus"
)

type SaverEx3 struct {
	c                 *SaverConfig
	journeyDispatcher []*dispatcher.JourneyDispatcher
	savers            []*SaverForEx3
	getter            *getters.Getter
	qMiddleware       *middleware.QueueMiddleware
	channels          []chan *dataStructures.Message
	finishedSignals   chan bool
	canSendGetter     chan bool
}

// NewSaverEx3 Creates a new exercise 4 handler
func NewSaverEx3(c *SaverConfig) *SaverEx3 {
	canSend := make(chan bool)
	var channels []chan *dataStructures.Message
	qMiddleware := middleware.NewQueueMiddleware(c.RabbitAddress)
	// We create the input queue to the EX4 service
	inputQueue := protocol.NewConsumerQueueProtocolHandler(qMiddleware.CreateConsumer(c.InputQueueName, true))
	prodToInput := protocol.NewProducerQueueProtocolHandler(qMiddleware.CreateProducer(c.InputQueueName, true))

	// Creation of the JourneySavers, they handle the prices per journey
	var internalSavers []*SaverForEx3
	var outputFileNames []string
	finishSignal := make(chan bool)
	var toInternalSaversChannels []protocol.ProducerProtocolInterface
	log.Infof("Creating %v savers...", int(c.InternalSaversCount))
	for i := 0; i < int(c.InternalSaversCount); i++ {
		internalSaverChannel := make(chan *dataStructures.Message)
		channels = append(channels, internalSaverChannel)
		internalSavers = append(internalSavers, NewSaverForEx3(
			protocol.NewConsumerChannel(internalSaverChannel),
			c,
			finishSignal,
			i,
		))
		outputFileNames = append(outputFileNames, fmt.Sprintf("%v_%v.csv", c.OutputFilePrefix, i))
		toInternalSaversChannels = append(toInternalSaversChannels, protocol.NewProducerChannel(internalSaverChannel))
		log.Infof("Created Saver #%v correctly...", i)
	}

	// Creation of the dispatcher to the JourneySavers
	log.Infof("Creating dispatcher...")
	jd := []*dispatcher.JourneyDispatcher{
		dispatcher.NewJourneyDispatcher(inputQueue, prodToInput, toInternalSaversChannels),
		dispatcher.NewJourneyDispatcher(inputQueue, prodToInput, toInternalSaversChannels),
		dispatcher.NewJourneyDispatcher(inputQueue, prodToInput, toInternalSaversChannels),
		dispatcher.NewJourneyDispatcher(inputQueue, prodToInput, toInternalSaversChannels),
	}

	getterConf := getters.NewGetterConfig(c.ID, outputFileNames, c.GetterAddress, c.GetterBatchLines)
	getter, err := getters.NewGetter(getterConf, canSend)
	if err != nil {
		log.Fatalf("%s", err)
	}

	return &SaverEx3{
		c:                 c,
		journeyDispatcher: jd,
		qMiddleware:       qMiddleware,
		channels:          channels,
		savers:            internalSavers,
		getter:            getter,
		finishedSignals:   finishSignal,
		canSendGetter:     canSend,
	}
}

func (se3 *SaverEx3) handleFinishSignals() {
	quantityFinished := uint(0)
	for {
		<-se3.finishedSignals
		quantityFinished++
		if quantityFinished == se3.c.InternalSaversCount {
			log.Infof("All savers finished. Notifying getter that it is able to send results...")
			se3.canSendGetter <- true
			return
		}
	}
}

// StartHandler Starts the exercise 4 services as goroutines
func (se3 *SaverEx3) StartHandler() {
	log.Debugf("Number of savers is: %v", len(se3.savers))
	for idx, saver := range se3.savers {
		log.Infof("Spawning saver #%v", idx+1)
		go saver.SaveData()
	}
	log.Infof("Spawning Dispatcher...")
	for _, jd := range se3.journeyDispatcher {
		go jd.DispatchLoop()
	}

	log.Infof("Spawning Getter...")
	go se3.getter.ReturnResults()
	log.Infof("Spawning task to handle when all savers finish...")
	go se3.handleFinishSignals()
}

// Close Closes the handler of the exercise 4
func (se3 *SaverEx3) Close() {
	for idx, channel := range se3.channels {
		log.Infof("Closing channel #%v", idx)
		close(channel)
	}
	log.Infof("Closing getter")
	se3.getter.Close()
}
