package main

import "github.com/Max20050/go_message_broker/client"

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

	broker.Consume()
}
