package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Max20050/go_message_broker/client"
)

// This example demonstrates a fanout exchange.
// One publisher sends to a "logs" fanout exchange.
// Two queues ("log_console" and "log_file") are bound to it.
// Every message is delivered to BOTH queues.
func main() {
	broker, err := client.ConnectBroker("localhost", "8080", "root", "root")
	if err != nil {
		panic(err)
	}

	ch := broker.OpenChannel()

	// 1. Declare the fanout exchange.
	if err := ch.DeclareExchange("logs", "fanout"); err != nil {
		panic(err)
	}

	// 2. Declare both queues.
	if err := ch.DeclareQueue("log_console", 500); err != nil {
		panic(err)
	}
	if err := ch.DeclareQueue("log_file", 500); err != nil {
		panic(err)
	}

	time.Sleep(200 * time.Millisecond)

	// 3. Bind both queues to the fanout exchange (routing key is ignored).
	if err := ch.BindQueue("log_console", "logs", ""); err != nil {
		panic(err)
	}
	if err := ch.BindQueue("log_file", "logs", ""); err != nil {
		panic(err)
	}

	time.Sleep(200 * time.Millisecond)

	// 4. Publish – every message goes to both queues.
	counter := 0
	for {
		counter++
		logEntry := map[string]interface{}{
			"level":   "INFO",
			"message": fmt.Sprintf("Log entry #%d", counter),
		}
		time.Sleep(3 * time.Second)
		if err := ch.Publish(context.Background(), "logs", "", "Logger", logEntry); err != nil {
			fmt.Println("Publish error:", err)
		}
		fmt.Printf("Published log #%d to fanout exchange 'logs'\n", counter)
	}
}
