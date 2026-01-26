package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Max20050/go_message_broker/client"
)

type Email struct {
	From    string
	Subject string
	Content string
}

type Headers struct {
	Method    string    `json:"method"` // Publish/Consume
	Issuer    string    `json:"issuer"` //e.g: Backend
	QueueName string    `json:"queuename"`
	Context   string    `json:"context"`   // optional topic. e.g: Emails,messages
	Timestamp time.Time `json:"timestamp"` // Time
}

type Message struct {
	Head    Headers     `json:"headers"`
	PayLoad interface{} `json:"payload"`
}

func main() {

	broker, err := client.ConnectBroker("localhost", "8080")
	if err != nil {
		panic(err.Error())
	}

	msgs, err := broker.Consume("default", "Email reciever", true)
	if err != nil {
		panic(err.Error())
	}

	for {
		var email Message
		msg := <-msgs
		if err := json.Unmarshal(msg, &email); err != nil {
			log.Printf("❌ Error unmarshalling message: %v", err)
			continue
		}
		fmt.Println(email)
	}
}
