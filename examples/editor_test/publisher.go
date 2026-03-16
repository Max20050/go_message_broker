package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/Max20050/go_message_broker/client"
)

// This publisher assumes you've already created the following via the Topology Editor:
//
//   Exchange:  "events"  (type: topic)
//   Queue:     "billing" (bound with key: "payments.due")
//   Queue:     "audit"   (bound with key: "payments.*")
//
// Steps:
//   1. Open http://localhost:15672/editor
//   2. Add a Topic exchange named "events"
//   3. Add queues "billing" and "audit"
//   4. Connect "events" → "billing" with routing key "payments.due"
//   5. Connect "events" → "audit"   with routing key "payments.*"
//   6. Click Deploy
//   7. Run this publisher and the consumer(s)

func main() {
	broker, err := client.ConnectBroker("localhost", "8080", "root", "root")
	if err != nil {
		panic(err)
	}

	ch := broker.OpenChannel()

	// Simulate different payment events with various routing keys.
	events := []struct {
		routingKey string
		data       func(int) map[string]interface{}
	}{
		{
			routingKey: "payments.due",
			data: func(i int) map[string]interface{} {
				return map[string]interface{}{
					"type":     "payment_due",
					"amount":   float64(50+rand.Intn(500)) + 0.99,
					"customer": fmt.Sprintf("Customer_%d", i),
					"currency": "USD",
				}
			},
		},
		{
			routingKey: "payments.received",
			data: func(i int) map[string]interface{} {
				return map[string]interface{}{
					"type":     "payment_received",
					"amount":   float64(100+rand.Intn(400)) + 0.50,
					"customer": fmt.Sprintf("Customer_%d", i),
					"method":   "credit_card",
				}
			},
		},
		{
			routingKey: "payments.refund",
			data: func(i int) map[string]interface{} {
				return map[string]interface{}{
					"type":     "payment_refund",
					"amount":   float64(10+rand.Intn(100)) + 0.00,
					"customer": fmt.Sprintf("Customer_%d", i),
					"reason":   "product_return",
				}
			},
		},
	}

	counter := 0
	for {
		counter++
		// Pick a random event type
		ev := events[rand.Intn(len(events))]
		payload := ev.data(counter)

		err := ch.Publish(context.Background(), "events", ev.routingKey, "PaymentService", payload)
		if err != nil {
			fmt.Println("❌ Publish error:", err)
		} else {
			fmt.Printf("✅ [%s] Published → exchange 'events' | %v\n", ev.routingKey, payload)
		}

		time.Sleep(2 * time.Second)
	}
}
