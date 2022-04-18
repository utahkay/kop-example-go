package main

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func consumeKafka(c *kafka.Consumer, kafkaTopic string) {
	err := c.SubscribeTopics([]string{kafkaTopic}, nil)
	if err != nil {
		log.Printf("WARN [kafka-consumer] Kafka consumer could not subscribe to topic %s: %s\n", kafkaTopic, err)
	}

	run := true
	for run {
		ev := c.Poll(0)
		switch e := ev.(type) {
		case *kafka.Message:
			log.Printf("INFO [kafka-consumer] Received message on partition %s: %s\n", e.TopicPartition, string(e.Value))
		case kafka.PartitionEOF:
			log.Printf("INFO [kafka-consumer] Consumer reached %v\n", e)
		case kafka.Error:
			log.Printf("WARN [kafka-consumer] received an error event: %v\n", e)
			run = false
		default:
			//log.Printf("INFO Consumer ignored event %v\n", e)
		}
	}

	c.Close()
}
