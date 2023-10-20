package middleware

type ProducerInterface interface {
	Send(data []byte) error
}

type ConsumerInterface interface {
	Pop() ([]byte, bool)
	BindTo(nameExchange string, routingKey string, kind string) error
}
