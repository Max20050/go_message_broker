package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Max20050/go_message_broker/client"
)

type Email struct {
	From    string `json:"from"`
	Subject string `json:"subject"`
	Content string `json:"content"`
}

func main() {
	broker, err := client.ConnectBroker("localhost", "8080", "root", "root")
	if err != nil {
		panic(err.Error())
	}

	ch := broker.OpenChannel()

	// Declare the queue we want to consume from.
	if err := ch.DeclareQueue("emails", 1000); err != nil {
		panic(err)
	}
	time.Sleep(200 * time.Millisecond)

	msgs, err := ch.Consume("emails", "Email receiver", false)
	if err != nil {
		panic(err.Error())
	}

	for msg := range msgs {
		var decoded Email
		fmt.Println("Message headers:", msg.Head)
		if err := json.Unmarshal(msg.PayLoad, &decoded); err != nil {
			log.Printf("❌ Error unmarshalling message: %v", err)
			continue
		}
		fmt.Println(decoded)
		msg.Ack() // Manual ack
	}
}
