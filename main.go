package main

import (
	"DistribuidosTP1/config"
	"DistribuidosTP1/middleware"
	"fmt"
	"log"
)

func main() {
	v := config.InitConfig()
	qMiddleware := middleware.NewQueueMiddleware()
	if v.GetString("queue.type") == "c" {
		queue := qMiddleware.CreateConsumer("test", true)
		for i := 0; i < 50; i++ {
			data := queue.Pop()
			log.Printf(string(data))
		}
	} else {
		queue := qMiddleware.CreateProducer("test", true)
		for i := 0; i < 50; i++ {
			msg := fmt.Sprintf("Hello World! %v", i)
			log.Printf("Sending msg: %v", msg)
			queue.Send([]byte(msg))
		}

	}
}
