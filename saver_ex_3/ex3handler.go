package main

import (
	"fmt"
	dataStructures "github.com/brunograssano/Distribuidos-TP1/common/data_structures"
	"github.com/brunograssano/Distribuidos-TP1/common/dispatcher"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	"github.com/brunograssano/Distribuidos-TP1/common/getters"
	"github.com/brunograssano/Distribuidos-TP1/common/middleware"
	"github.com/brunograssano/Distribuidos-TP1/common/protocol"
	"github.com/brunograssano/Distribuidos-TP1/common/utils"
	log "github.com/sirupsen/logrus"
)

type Ex3Handler struct {
	c                 *SaverConfig
	journeyDispatcher []*dispatcher.JourneyDispatcher
	savers            []*SaverForEx3
	getter            *getters.Getter
	qMiddleware       *middleware.QueueMiddleware
	channels          []chan *dataStructures.Message
	finishedSignals   chan bool
	canSendGetter     chan string
	outputFilenames   []string
}

// NewEx3Handler Creates a new exercise 4 handler
func NewEx3Handler(c *SaverConfig) *Ex3Handler {
	canSend := make(chan string, 1)
	var channels []chan *dataStructures.Message
	qMiddleware := middleware.NewQueueMiddleware(c.RabbitAddress)

	// Creation of the JourneySavers, they handle the prices per journey
	var internalSavers []*SaverForEx3
	var outputFileNames []string
	finishSignal := make(chan bool, c.InternalSaversCount)
	var toInternalSaversChannels []protocol.ProducerProtocolInterface
	log.Infof("Ex3Handler | Creating %v savers...", int(c.InternalSaversCount))
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
		log.Infof("Ex3Handler | Created Saver #%v correctly...", i)
	}

	// Creation of the dispatcher to the JourneySavers
	log.Infof("Ex3Handler | Creating dispatchers...")
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
		log.Fatalf("Ex3Handler | Error initializing Getter | %s", err)
	}

	return &Ex3Handler{
		c:                 c,
		journeyDispatcher: jds,
		qMiddleware:       qMiddleware,
		channels:          channels,
		savers:            internalSavers,
		getter:            getter,
		finishedSignals:   finishSignal,
		canSendGetter:     canSend,
		outputFilenames:   outputFileNames,
	}
}

func (se3 *Ex3Handler) handleFinishSignals() {
	quantityFinished := uint(0)
	for {
		_, ok := <-se3.finishedSignals
		// If channels are closed is because I received a Close
		if !ok {
			return
		}
		quantityFinished++
		if quantityFinished == se3.c.InternalSaversCount {
			log.Infof("Ex3Handler | All savers finished | Notifying getter that it is able to send results...")
			folder, err := filemanager.MoveFiles(se3.outputFilenames)
			if err != nil {
				log.Errorf("Ex3Handler | Error trying to move files into folder | %v", err)
			}
			se3.canSendGetter <- folder
			quantityFinished = 0
		}
	}
}

// StartHandler Starts the exercise 4 services as goroutines
func (se3 *Ex3Handler) StartHandler() {
	log.Debugf("Ex3Handler | Number of savers: %v", len(se3.savers))
	for idx, saver := range se3.savers {
		log.Infof("Ex3Handler | Spawning saver #%v", idx+1)
		go saver.SaveData()
	}

	log.Debugf("Ex3Handler | Number of Dispatchers: %v", len(se3.journeyDispatcher))
	for idx, jd := range se3.journeyDispatcher {
		log.Infof("Ex3Handler | Spawning Dispatcher #%v", idx)
		go jd.DispatchLoop()
	}

	log.Infof("Ex3Handler | Spawning Getter...")
	go se3.getter.ReturnResults()
	log.Infof("Ex3Handler | Spawning task to handle when all savers finish...")
	go se3.handleFinishSignals()
}

// Close Closes the handler of the exercise 4
func (se3 *Ex3Handler) Close() {
	log.Infof("Ex3Handler | Closing resources...")
	log.Infof("Ex3Handler | Starting channel closing...")
	for idx, channel := range se3.channels {
		log.Infof("Ex3Handler | Closing channel #%v", idx)
		close(channel)
	}
	close(se3.finishedSignals)
	log.Infof("Ex3Handler | Closing Getter")
	se3.getter.Close()
	log.Infof("Ex3Handler | Ended closing resources")
}
