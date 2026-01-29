package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/Max20050/go_message_broker/client"
)

type Email struct {
	From    string `json:"from"`
	Subject string `json:"subject"`
	Content string `json:"content"`
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
		var decoded Email
		msg := <-msgs
		fmt.Println("Message headers: ", msg.Head)
		if err := json.Unmarshal(msg.PayLoad, &decoded); err != nil {
			log.Printf("❌ Error unmarshalling message: %v", err)
			continue
		}
		fmt.Println(decoded)
	}
}
