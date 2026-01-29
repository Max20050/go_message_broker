package client

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/google/uuid"
)

// Message headers
type Headers struct {
	Method    string    `json:"method"` // Publish/Consume
	Issuer    string    `json:"issuer"` //e.g: Backend
	QueueName string    `json:"queuename"`
	Context   string    `json:"context"`
	Timestamp time.Time `json:"timestamp"` // Add this field
}

// Full message sent to the broker
type MessagePublisher struct {
	Head    Headers         `json:"headers"`
	PayLoad json.RawMessage `json:"payload"`
}

type FullHeaders struct {
	MessageId uuid.UUID `json:"message_id"`
	Method    string    `json:"method"`
	Issuer    string    `json:"issuer"` //e.g: Backend
	QueueName string    `json:"queuename"`
	Context   string    `json:"context"`
	Timestamp time.Time `json:"timestamp"` // Add this field
}

// Full message sent to the broker
type MessageConsumer struct {
	Head    FullHeaders     `json:"headers"`
	PayLoad json.RawMessage `json:"payload"`
	Broker  *Broker         `json:"-"`
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
	// Marshal the message to JSON bytes
	payloadBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}

	msg := MessagePublisher{
		Head: Headers{
			Method:    "PUBLISH",
			Issuer:    publisher,
			QueueName: Qname,
			Context:   topic,
			Timestamp: time.Now(),
		},
		PayLoad: json.RawMessage(payloadBytes), // This should be []byte containing JSON
	}

	// Marshal the entire message
	fullMessageBytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	// Send with newline for scanner
	fullMessageBytes = append(fullMessageBytes, '\n')

	_, err = b.connection.Write(fullMessageBytes)
	return err
}

func (b *Broker) Consume(QueueName string, ConsumerTag string, AutoAck bool) (chan MessageConsumer, error) {

	msg := make(chan MessageConsumer) // handle server push

	request := MessagePublisher{
		Head: Headers{
			Method:    "CONSUME",
			Issuer:    ConsumerTag,
			QueueName: "default",
			Context:   "main",
		}, // TODO: Add the config into the payload here
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
			var message MessageConsumer
			if err := json.Unmarshal(data, &message); err != nil {
				fmt.Printf("Error unmarshalling JSON: %v\n", err)
				fmt.Printf("JSON was: %s\n", string(data))
				continue
			}
			message.Broker = b
			msg <- message
		}
	}()
	return msg, nil
}

func (m *MessageConsumer) Ack() error {
	request := MessagePublisher{
		Head: Headers{
			Method:    "ACK",
			Issuer:    "Backend",
			QueueName: "default",
			Context:   "main",
		}, // TODO: Add the consumertag in the payload for later use
	}

	messageID, err := GetBytes(m.Head.MessageId)
	if err != nil {
		panic(err.Error())
	}
	request.PayLoad = json.RawMessage(messageID)
	encoder := json.NewEncoder(m.Broker.connection)
	err = encoder.Encode(request)
	if err != nil {
		fmt.Println("Send error:", err)
		return nil
	}
	return nil
}

func GetBytes(key any) ([]byte, error) {
	return json.Marshal(key)
}
