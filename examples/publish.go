package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Max20050/go_message_broker/client"
)

func main() {
	broker, err := client.ConnectBroker("localhost", "8080")
	if err != nil {
		panic(err.Error())
	}

	ch := broker.OpenChannel()

	// Declare the queue we want to publish to.
	if err := ch.DeclareQueue("emails", 1000); err != nil {
		panic(err)
	}
	time.Sleep(200 * time.Millisecond) // let the server process

	counter := 0
	for {
		counter++
		em := map[string]interface{}{
			"from":    "maxmimoabella12@gmail.com",
			"subject": "Example Email",
			"content": fmt.Sprintf("This is the email number: %d", counter),
		}
		time.Sleep(time.Second * 5)

		// Point-to-point: publish to the default exchange with routing key = queue name.
		if err := ch.Publish(context.Background(), "", "emails", "Backend 1", em); err != nil {
			fmt.Println("Publish error:", err)
		}
	}
}
