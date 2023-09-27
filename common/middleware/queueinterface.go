package middleware

type ProducerInterface interface {
	Send(data []byte)
}

type ConsumerInterface interface {
	Pop() []byte
}
