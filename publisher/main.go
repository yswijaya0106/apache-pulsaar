package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

const (
	pulsarURL = "pulsar://172.29.88.203:6650"
	topic     = "persistent://public/default/demo-redelivery"
)

func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               pulsarURL,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("could not create pulsar client: %v", err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})
	if err != nil {
		log.Fatalf("could not create producer: %v", err)
	}
	defer producer.Close()

	for i := 1; i <= 5; i++ {
		payload := fmt.Sprintf("message-%d", i)
		msgID, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte(payload),
		})
		if err != nil {
			log.Printf("[PRODUCER] failed to send %q: %v", payload, err)
			continue
		}
		log.Printf("[PRODUCER] sent: %q  msgID=%v", payload, msgID)
		time.Sleep(500 * time.Millisecond)
	}

	log.Println("[PRODUCER] done")
}
