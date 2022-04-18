package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
)

func produce(p *kafka.Producer, kafkaTopic string) {
	produceIntervalMs := 2000
	delivery_chan := make(chan kafka.Event)

	for i := 0; ; i++ {

		message := fmt.Sprintf("message %d, %s", i, uuid.New())

		_ = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
			Value:          []byte(message)},
			delivery_chan,
		)

		e := <-delivery_chan
		m := e.(*kafka.Message)

		if m.TopicPartition.Error != nil {
			log.Printf("WARN [producer] Delivery failed: %v\n", m.TopicPartition.Error)
		} else {
			log.Printf("INFO [producer] Delivered message %s to topic %s [%d] at offset %v\n",
				string(m.Value), *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
		}

		time.Sleep(time.Duration(produceIntervalMs) * time.Millisecond)
	}
}
