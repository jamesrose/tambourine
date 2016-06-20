package main

import (
	"time"

	"testing"

	"github.com/jamesrose/tambourine"
)

func BenchmarkPublish(b *testing.B) {
	adapter := tambourine.NewSNSSQSAdapter(tambourine.SNSSQSConfig{
		QueueNamePrefix: "jamesr",
		Region:          "eu-west-1",
	})
	session := tambourine.NewSession(adapter)
	for i := 0; i < b.N; i++ {
		msg := time.Now().Format("Hello World - Jan 2 15:04:05")

		err := session.Publish(queue, tambourine.Message{Body: msg})
		if err != nil {
			panic(err)
		}
	}
}
