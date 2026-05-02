package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	eventlib "gitlab.smartfren.com/sharedproject/event-publisher/eventlib"
	clientlib "gitlab.smartfren.com/sharedproject/event-publisher/eventlib/client"
	cm "gitlab.smartfren.com/sharedproject/event-publisher/eventlib/common"
)

const (
	pulsarURL    = "pulsar://172.29.50.54:6650"
	topic        = "persistent://public/default/demo-redelivery"
	subscription = "demo-sub"
)

func main() {
	cfg := &cm.Configuration{
		ClientType:              "pulsar",
		Host:                    pulsarURL,
		AllowInsecureConnection: true,
	}

	evClient, err := eventlib.NewClient(cfg)
	if err != nil {
		log.Fatalf("could not create client: %v", err)
	}
	defer evClient.Close()

	subscriber := evClient.CreateSubscriber(subscription)
	defer subscriber.Close()

	var seen sync.Map

	if err := subscriber.SubscribeV2(topic, clientlib.Shared, false, func(msg clientlib.Message, ack func(ok bool)) {
		key := string(msg.ID)
		payload := string(msg.Payload)

		if _, alreadyNacked := seen.LoadOrStore(key, true); !alreadyNacked {
			log.Printf("[CONSUMER] ATTEMPT-1 received: %q  msgID=%x  → NACK (akan dikirim ulang)", payload, msg.ID)
			ack(false)
		} else {
			log.Printf("[CONSUMER] ATTEMPT-2 redelivered: %q  msgID=%x  → ACK", payload, msg.ID)
			ack(true)
			seen.Delete(key)
		}
	}); err != nil {
		log.Fatalf("could not subscribe: %v", err)
	}

	log.Println("[CONSUMER] waiting for messages... (Ctrl+C to stop)")
	go subscriber.Start()

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt, syscall.SIGTERM)
	<-terminate
}
