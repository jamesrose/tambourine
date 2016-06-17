package main

import (
	"fmt"
	"github.com/jamesrose/tambourine"
)

func main() {
	adapter := tambourine.NewSNSSQSAdapter(tambourine.SNSSQSConfig{
		QueueNamePrefix: "jamesr",
		Region:          "eu-west-1",
	})
	session := tambourine.NewSession(adapter)

	queue := tambourine.Queue{Name: "my_queue"}

	err := session.Publish(
		queue,
		tambourine.Message{Body: "Hello World!"},
	)
	if err != nil {
		panic(err)
	}

	msg, err := session.Consume(queue, "my_worker")
	if err != nil {
		panic(err)
	}

	fmt.Printf("Messages: %#v", msg)
}
