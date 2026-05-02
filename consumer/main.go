package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

const (
	pulsarURL    = "pulsar://172.29.88.203:6650"
	topic        = "persistent://public/default/demo-redelivery"
	subscription = "demo-sub"
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

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subscription,
		Type:             pulsar.Shared,
		// Nacked messages akan dikirim ulang setelah 3 detik
		NackRedeliveryDelay: 3 * time.Second,
	})
	if err != nil {
		log.Fatalf("could not subscribe: %v", err)
	}
	defer consumer.Close()

	log.Println("[CONSUMER] waiting for messages... (Ctrl+C to stop)")

	// seen menyimpan message ID string yang sudah di-nack (attempt pertama)
	var seen sync.Map

	ctx := context.Background()
	for {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			log.Printf("[CONSUMER] receive error: %v", err)
			return
		}

		key := msg.ID().String()
		payload := string(msg.Payload())

		if _, alreadyNacked := seen.LoadOrStore(key, true); !alreadyNacked {
			// Attempt pertama: sengaja NACK supaya Pulsar kirim ulang
			log.Printf("[CONSUMER] ATTEMPT-1 received: %q  msgID=%v  → NACK (tidak di-ack, akan dikirim ulang)", payload, key)
			consumer.Nack(msg)
		} else {
			// Attempt kedua (redelivery): ack sekarang
			log.Printf("[CONSUMER] ATTEMPT-2 redelivered: %q  msgID=%v  → ACK", payload, key)
			if err := consumer.Ack(msg); err != nil {
				log.Printf("[CONSUMER] ack error: %v", err)
			}
			seen.Delete(key)
		}
	}
}
