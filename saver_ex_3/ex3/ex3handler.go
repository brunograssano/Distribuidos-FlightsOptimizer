package ex3

import (
	"fmt"
	"github.com/brunograssano/Distribuidos-TP1/common/checkpointer"
	"github.com/brunograssano/Distribuidos-TP1/common/dispatcher"
	"github.com/brunograssano/Distribuidos-TP1/common/filemanager"
	"github.com/brunograssano/Distribuidos-TP1/common/getters"
	queueProtocol "github.com/brunograssano/Distribuidos-TP1/common/protocol/queues"
	"github.com/brunograssano/Distribuidos-TP1/common/queuefactory"
	log "github.com/sirupsen/logrus"
)

type Ex3Handler struct {
	c                        *SaverConfig
	journeyDispatcher        []*dispatcher.JourneyDispatcher
	savers                   []*SaverForEx3
	getter                   *getters.Getter
	finishedSignals          chan string
	outputFilenames          []string
	quantityFinishedByClient map[string]uint
}

// NewEx3Handler Creates a new exercise 3 handler
func NewEx3Handler(c *SaverConfig, dispatchersQFactory queuefactory.QueueProtocolFactory, internalQFactory queuefactory.QueueProtocolFactory) *Ex3Handler {

	// Creation of the JourneySavers, they handle the prices per journey
	var internalSaversConsumers []*SaverForEx3
	var outputFileNames []string
	finishSignal := make(chan string, c.InternalSaversCount)
	var toInternalSavers []queueProtocol.ProducerProtocolInterface
	log.Infof("Ex3Handler | Creating %v savers...", int(c.InternalSaversCount))
	for i := 0; i < int(c.InternalSaversCount); i++ {
		checkpointerHandler := checkpointer.NewCheckpointerHandler()
		internalSaversConsumers = append(internalSaversConsumers, NewSaverForEx3(
			internalQFactory.CreateConsumer(fmt.Sprintf("saver3-internal-%v-%v", c.ID, i)),
			c,
			finishSignal,
			i,
			checkpointerHandler,
		))
		outputFileNames = append(outputFileNames, fmt.Sprintf("%v_%v", c.OutputFilePrefix, i))
		toInternalSavers = append(toInternalSavers, internalQFactory.CreateProducer(fmt.Sprintf("saver3-internal-%v-%v", c.ID, i)))
		checkpointerHandler.RestoreCheckpoint()
		log.Infof("Ex3Handler | Created Saver #%v correctly...", i)
	}

	// Creation of the dispatcher to the JourneySavers
	log.Infof("Ex3Handler | Creating dispatchers...")
	var jds []*dispatcher.JourneyDispatcher
	for i := uint(0); i < c.DispatchersCount; i++ {
		checkpointerHandler := checkpointer.NewCheckpointerHandler()
		// We create the input queue to the EX3 service
		inputQueue := dispatchersQFactory.CreateConsumer(fmt.Sprintf("%v-%v", c.InputQueueName, c.ID))
		prodToInput := dispatchersQFactory.CreateProducer(c.ID)
		tmpDispatcher := dispatcher.NewJourneyDispatcher(i, inputQueue, prodToInput, toInternalSavers, checkpointerHandler)
		jds = append(jds, tmpDispatcher)
		checkpointerHandler.RestoreCheckpoint()
	}

	getterConf := getters.NewGetterConfig(c.ID, outputFileNames, c.GetterAddress, c.GetterBatchLines)
	getter, err := getters.NewGetter(getterConf)
	if err != nil {
		log.Fatalf("Ex3Handler | Error initializing Getter | %s", err)
	}

	return &Ex3Handler{
		c:                        c,
		journeyDispatcher:        jds,
		savers:                   internalSaversConsumers,
		getter:                   getter,
		finishedSignals:          finishSignal,
		outputFilenames:          outputFileNames,
		quantityFinishedByClient: make(map[string]uint),
	}
}

func (se3 *Ex3Handler) handleFinishSignals() {
	for {
		clientId, ok := <-se3.finishedSignals
		// If channels are closed is because I received a Close
		if !ok {
			return
		}
		_, existsCID := se3.quantityFinishedByClient[clientId]
		if !existsCID {
			se3.quantityFinishedByClient[clientId] = 0
		}
		se3.quantityFinishedByClient[clientId]++
		if se3.quantityFinishedByClient[clientId] == se3.c.InternalSaversCount {
			log.Infof("Ex3Handler | All savers finished | Notifying getter that it is able to send results...")
			//Renames the folder from tmp to definitive folder
			err := filemanager.RenameFile(fmt.Sprintf("%v_tmp", clientId), fmt.Sprintf("%v", clientId))
			if err != nil {
				log.Errorf("Ex3Handler | Error trying to rename client_id %v folder | %v", clientId, err)
			}
			delete(se3.quantityFinishedByClient, clientId)
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
	close(se3.finishedSignals)
	log.Infof("Ex3Handler | Closing Getter")
	se3.getter.Close()
	log.Infof("Ex3Handler | Ended closing resources")
}
