package main

import (
	"fmt"
	"log"
	"time"

	eventlib "gitlab.smartfren.com/sharedproject/event-publisher/eventlib"
	clientlib "gitlab.smartfren.com/sharedproject/event-publisher/eventlib/client"
)

const (
	pulsarURL     = "pulsar://172.29.50.54:6650"
	topic         = "persistent://public/default/demo-redelivery"
	publisherName = "demo-publisher"
)

func main() {
	publisher, err := eventlib.NewPublisher(clientlib.Pulsar, pulsarURL, "", "", true, publisherName, topic)
	if err != nil {
		log.Fatalf("could not create publisher: %v", err)
	}
	defer func() {
		publisher.Close()
		publisher.GetClient().Close()
	}()

	for i := 1; i <= 5; i++ {
		payload := fmt.Sprintf("message-%d", i)
		if err := publisher.Send(topic, []byte(payload), nil); err != nil {
			log.Printf("[PRODUCER] failed to send %q: %v", payload, err)
			continue
		}
		log.Printf("[PRODUCER] sent: %q", payload)
		time.Sleep(500 * time.Millisecond)
	}

	log.Println("[PRODUCER] done")
}
