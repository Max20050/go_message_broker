package client

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
)

// Message headers
type Headers struct {
	Method    string `json:"method"` // Publish/Consume
	Issuer    string `json:"issuer"` //e.g: Backend
	QueueName string `json:"queuename"`
	Context   string `json:"context"`
}

// Full message sent to the broker
type Message struct {
	Head    Headers     `json:"headers"`
	PayLoad interface{} `json:"payload"`
}

type Broker struct {
	port       string
	address    string
	connection net.Conn
}

func ConnectBroker(address string, port string) (Broker, error) {
	conn, err := net.Dial("tcp", address+":"+port)
	if err != nil {
		return Broker{}, err
	}
	return Broker{
		port:       port,
		address:    address,
		connection: conn,
	}, nil
}

func (b *Broker) Publish(ctx context.Context, publisher string, topic string, Qname string, message interface{}) error {
	msg := Message{
		Head: Headers{
			Method:    "PUBLISH",
			Issuer:    publisher,
			QueueName: Qname,
			Context:   topic,
		},
		PayLoad: message,
	}

	encoder := json.NewEncoder(b.connection)
	err := encoder.Encode(msg)
	if err != nil {
		return err
	}
	return nil
}

func (b *Broker) Consume(QueueName string, ConsumerTag string, AutoAck bool) (chan []byte, error) {

	msg := make(chan []byte)

	request := Message{
		Head: Headers{
			Method:    "CONSUME",
			Issuer:    "Backend",
			QueueName: "default",
			Context:   "main",
		},
		PayLoad: "Hello from client",
	}

	encoder := json.NewEncoder(b.connection)
	err := encoder.Encode(request)
	if err != nil {
		fmt.Println("Send error:", err)
		return nil, err
	}
	reader := bufio.NewReader(b.connection)
	go func() {
		for {
			// Read data from server
			data, err := reader.ReadBytes('\n')
			if err != nil {
				fmt.Println("Connection closed by server")
				return
			}
			msg <- data
		}
	}()
	return msg, nil
}
