package middleware

type ProducerInterface interface {
	Send(data []byte) error
	GetName() string
}

type ConsumerInterface interface {
	Pop() ([]byte, bool)
	BindTo(nameExchange string, routingKey string, kind string) error
	SignalFinishedMessage(processedCorrectly bool) error
	GetName() string
}
