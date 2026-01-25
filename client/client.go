package client

import (
	"context"
	"encoding/json"
	"net"
)

type Headers struct {
	Method    string `json:"method"` // Publish/Consume
	Issuer    string `json:"issuer"` //e.g: Backend
	QueueName string `json:"queuename"`
	Context   string `json:"context"`
}

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
