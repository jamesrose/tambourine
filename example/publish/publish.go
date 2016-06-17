package main

import (
	"fmt"
	"time"

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
		msg := time.Now().Format("Hello World - Jan 2 15:04:05")

		err := session.Publish(queue, tambourine.Message{Body: msg})
		if err != nil {
			panic(err)
		}
		fmt.Println("Published Message:", msg)
		time.Sleep(1 * time.Second)
	}

}
