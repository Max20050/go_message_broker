package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Max20050/go_message_broker/client"
)

// This example demonstrates a topic exchange.
// Messages with routing key "payments.due" go to the "billing" queue.
// Messages with routing key "payments.*" (wildcard) go to the "audit" queue.
func main() {
	broker, err := client.ConnectBroker("localhost", "8080", "root", "root")
	if err != nil {
		panic(err)
	}

	ch := broker.OpenChannel()

	// 1. Declare the topic exchange.
	if err := ch.DeclareExchange("events", "topic"); err != nil {
		panic(err)
	}

	// 2. Declare queues.
	if err := ch.DeclareQueue("billing", 500); err != nil {
		panic(err)
	}
	if err := ch.DeclareQueue("audit", 500); err != nil {
		panic(err)
	}

	time.Sleep(200 * time.Millisecond)

	// 3. Bind with topic patterns.
	//    "billing" only gets "payments.due"
	//    "audit" gets everything under "payments.*"
	if err := ch.BindQueue("billing", "events", "payments.due"); err != nil {
		panic(err)
	}
	if err := ch.BindQueue("audit", "events", "payments.*"); err != nil {
		panic(err)
	}

	time.Sleep(200 * time.Millisecond)

	// 4. Publish different events.
	events := []struct {
		key  string
		body map[string]interface{}
	}{
		{"payments.due", map[string]interface{}{"amount": 150.00, "customer": "Alice"}},
		{"payments.received", map[string]interface{}{"amount": 150.00, "customer": "Alice"}},
		{"payments.due", map[string]interface{}{"amount": 300.00, "customer": "Bob"}},
		{"payments.refund", map[string]interface{}{"amount": 50.00, "customer": "Charlie"}},
	}

	for _, ev := range events {
		if err := ch.Publish(context.Background(), "events", ev.key, "PaymentService", ev.body); err != nil {
			fmt.Println("Publish error:", err)
		}
		fmt.Printf("Published %s → exchange 'events'\n", ev.key)
		time.Sleep(2 * time.Second)
	}

	fmt.Println("Done publishing.")
}
