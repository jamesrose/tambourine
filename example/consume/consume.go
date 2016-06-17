package main

import (
	"fmt"

	"github.com/jamesrose/tambourine"
)

var (
	queue = tambourine.Queue{Name: "my_queue"}
)

func main() {
	adapter := tambourine.NewSNSSQSAdapter(tambourine.SNSSQSConfig{
		QueueNamePrefix: "jamesr",
		Region:          "eu-west-1",
	})
	session := tambourine.NewSession(adapter)

	for {
		msg, err := session.Consume(queue, "my_worker")
		if err != nil {
			panic(err)
		}

		if len(msg) > 0 {
			fmt.Printf("Received Messages: %#v\n", msg)
		}

	}
}
