package main

import (
	"context"
	"time"

	"github.com/Max20050/go_message_broker/client"
)

type Email struct {
	From    string
	Subject string
	Content string
}

func main() {

	broker, err := client.ConnectBroker("localhost", "8080")
	if err != nil {
		panic(err.Error())
	}
	em := map[string]interface{}{
		"from":    "maxmimoabella12@gmail.com",
		"subject": "Example Email",
		"content": "This is an example email",
	}
	for {
		time.Sleep(time.Second * 5) // Wait 5 and send message to queue
		broker.Publish(context.Background(), "Backend 1", "Emails", "default", em)
	}
}
