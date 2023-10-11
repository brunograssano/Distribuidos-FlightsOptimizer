package main

import (
	"fmt"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/dispatcher"
	"github.com/brunograssano/Distribuidos-TP1/common/getters"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
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
	canSend := make(chan bool, 1)
	var channels []chan *dataStructures.Message
	qMiddleware := middleware.NewQueueMiddleware(c.RabbitAddress)

	// Creation of the JourneySavers, they handle the prices per journey
	var internalSavers []*SaverForEx3
	var outputFileNames []string
	finishSignal := make(chan bool, c.InternalSaversCount)
	var toInternalSaversChannels []protocol.ProducerProtocolInterface
	log.Infof("SaverEx3 | Creating %v savers...", int(c.InternalSaversCount))
	for i := 0; i < int(c.InternalSaversCount); i++ {
		internalSaverChannel := make(chan *dataStructures.Message, utils.BufferSizeChannels)
		channels = append(channels, internalSaverChannel)
		internalSavers = append(internalSavers, NewSaverForEx3(
			protocol.NewConsumerChannel(internalSaverChannel),
			c,
			finishSignal,
			i,
		))
		outputFileNames = append(outputFileNames, fmt.Sprintf("%v_%v.csv", c.OutputFilePrefix, i))
		toInternalSaversChannels = append(toInternalSaversChannels, protocol.NewProducerChannel(internalSaverChannel))
		log.Infof("SaverEx3 | Created Saver #%v correctly...", i)
	}

	// Creation of the dispatcher to the JourneySavers
	log.Infof("SaverEx3 | Creating dispatchers...")
	var jds []*dispatcher.JourneyDispatcher
	for i := uint(0); i < c.DispatchersCount; i++ {
		// We create the input queue to the EX4 service
		inputQueue := protocol.NewConsumerQueueProtocolHandler(qMiddleware.CreateConsumer(c.InputQueueName, true))
		prodToInput := protocol.NewProducerQueueProtocolHandler(qMiddleware.CreateProducer(c.InputQueueName, true))
		jds = append(jds, dispatcher.NewJourneyDispatcher(inputQueue, prodToInput, toInternalSaversChannels))
	}

	getterConf := getters.NewGetterConfig(c.ID, outputFileNames, c.GetterAddress, c.GetterBatchLines)
	getter, err := getters.NewGetter(getterConf, canSend)
	if err != nil {
		log.Fatalf("SaverEx3 | Error initializing Getter | %s", err)
	}

	return &SaverEx3{
		c:                 c,
		journeyDispatcher: jds,
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
			log.Infof("SaverEx3 | All savers finished | Notifying getter that it is able to send results...")
			se3.canSendGetter <- true
			return
		}
	}
}

// StartHandler Starts the exercise 4 services as goroutines
func (se3 *SaverEx3) StartHandler() {
	log.Debugf("SaverEx3 | Number of savers: %v", len(se3.savers))
	for idx, saver := range se3.savers {
		log.Infof("SaverEx3 | Spawning saver #%v", idx+1)
		go saver.SaveData()
	}

	log.Debugf("SaverEx3 | Number of Dispatchers: %v", len(se3.journeyDispatcher))
	for idx, jd := range se3.journeyDispatcher {
		log.Infof("SaverEx3 | Spawning Dispatcher #%v", idx)
		go jd.DispatchLoop()
	}

	log.Infof("SaverEx3 | Spawning Getter...")
	go se3.getter.ReturnResults()
	log.Infof("SaverEx3 | Spawning task to handle when all savers finish...")
	go se3.handleFinishSignals()
}

// Close Closes the handler of the exercise 4
func (se3 *SaverEx3) Close() {
	log.Infof("SaverEx3 | Closing resources...")
	log.Infof("SaverEx3 | Starting channel closing...")
	for idx, channel := range se3.channels {
		log.Infof("SaverEx3 | Closing channel #%v", idx)
		close(channel)
	}
	log.Infof("SaverEx3 | Closing Getter")
	se3.getter.Close()
	log.Infof("SaverEx3 | Ended closing resources")
}
