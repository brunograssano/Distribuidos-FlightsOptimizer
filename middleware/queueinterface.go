package middleware

// TODO separar en productor y consumidor
type QueueInterface interface {
	Send(data []byte)
	Pop() []byte
	BecomeConsumer()
}
