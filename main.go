package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	fmt.Println("Hello to KoP")

	bootstrapServers := "127.0.0.1:19092"
	clientId := "producer-1"
	topic := "public/kafka/test-topic"
	consumerGroupId := "consumer-1"
	pulsarServiceUrl := "pulsar://localhost:6650"
	pulsarSubscriptionName := "subscription-1"
	prometheusMetricsPort := 8081

	log.Printf("INFO Creating Kafka producer with clientId %s on bootstrapServers %s\n", clientId, bootstrapServers)
	kafkaProducer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"client.id":         clientId,
		"acks":              "all"})
	if err != nil {
		log.Fatalf("Failed to create producer: %s\n", err)
	}
	log.Printf("INFO Created Kafka producer")

	log.Printf("INFO Creating Kafka consumer with groupId %s on bootstrapServers %s\n", consumerGroupId, bootstrapServers)
	kafkaConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          consumerGroupId,
		"auto.offset.reset": "smallest",
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s\n", err)
	}
	log.Printf("INFO Created Kafka consumer")

	log.Printf("INFO Creating Pulsar client")
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: pulsarServiceUrl,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	fmt.Println("INFO Created Pulsar client successfully")
	log.Printf("INFO Creating Pulsar consumer")
	pulsarConsumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: pulsarSubscriptionName,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatalf("ERROR Failed to create Pulsar consumer: %s", err)
	}
	defer pulsarConsumer.Close()
	log.Printf("INFO Created Pulsar consumer successfully")

	go produce(kafkaProducer, topic)
	go consumeKafka(kafkaConsumer, topic)
	go consumePulsar(pulsarConsumer)

	log.Printf("INFO Starting Prometheus metrics server on port %d\n", prometheusMetricsPort)
	http.Handle("/metrics", promhttp.Handler())
	err = http.ListenAndServe(fmt.Sprintf(":%d", prometheusMetricsPort), nil)
	if err != nil {
		log.Fatalf("ERROR Failed to start Prometheus metrics server: %s", err)
	}
}
