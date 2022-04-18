package main

import (
	"context"
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
)

func consumePulsar(consumer pulsar.Consumer) {
	log.Printf("INFO [pulsar-consumer] Ready to consume messages")

	for {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		consumer.Ack(msg)

		payload := string(msg.Payload())
		log.Printf("INFO [pulsar-consumer] Received message msgId: %v -- content: '%v'\n",
			msg.ID(), payload)
	}
}
