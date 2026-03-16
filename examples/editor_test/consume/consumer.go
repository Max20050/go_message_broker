package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Max20050/go_message_broker/client"
)

// This consumer listens on a queue that was created via the Topology Editor.
//
// Usage:
//   go run consumer.go billing     ← only receives "payments.due"
//   go run consumer.go audit       ← receives "payments.*" (due, received, refund)
//
// Setup (in the editor):
//   1. Open http://localhost:15672/editor
//   2. Create a Topic exchange "events"
//   3. Create queues "billing" and "audit"
//   4. Bind "billing" to "events" with routing key "payments.due"
//   5. Bind "audit" to "events" with routing key "payments.*"
//   6. Deploy
//   7. Run this consumer, then the publisher

func main() {
	queueName := "audit"
	if len(os.Args) > 1 {
		queueName = os.Args[1]
	}

	broker, err := client.ConnectBroker("localhost", "8080", "root", "root")
	if err != nil {
		panic(err)
	}

	ch := broker.OpenChannel()

	// No DeclareQueue here — the queue was created via the editor.
	// Just start consuming.
	tag := fmt.Sprintf("%s_consumer", queueName)
	msgs, err := ch.Consume(queueName, tag, false)
	if err != nil {
		panic(err)
	}

	fmt.Printf("🎧 Listening on queue %q (tag: %s)\n", queueName, tag)
	fmt.Println("   Waiting for messages... (Ctrl+C to quit)")
	fmt.Println()

	for msg := range msgs {
		// Pretty-print the payload
		var payload map[string]interface{}
		if err := json.Unmarshal(msg.PayLoad, &payload); err != nil {
			log.Printf("❌ Error decoding message: %v", err)
			msg.Ack()
			continue
		}

		prettyPayload, _ := json.MarshalIndent(payload, "   ", "  ")

		fmt.Printf("📨 [%s] Message received:\n", msg.Head.Routing)
		fmt.Printf("   ID:      %s\n", msg.Head.MessageId)
		fmt.Printf("   Issuer:  %s\n", msg.Head.Issuer)
		fmt.Printf("   Key:     %s\n", msg.Head.Routing)
		fmt.Printf("   Payload: %s\n", string(prettyPayload))
		fmt.Println()

		// Simulate processing time
		time.Sleep(500 * time.Millisecond)
		msg.Ack()
		fmt.Printf("   ✅ ACKed %s\n\n", msg.Head.MessageId)
	}
}
