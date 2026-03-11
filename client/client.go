package client

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// -----------------------------------------------------------------------
// Wire-level types (client-side mirrors of server models)
// -----------------------------------------------------------------------

// Headers sent on every request.
type Headers struct {
	Method    string    `json:"method"`
	Issuer    string    `json:"issuer"`
	Exchange  string    `json:"exchange"`
	Routing   string    `json:"routing"`
	ChannelID int       `json:"channel_id"`
	Timestamp time.Time `json:"timestamp"`
}

// MessagePublisher is the full frame sent from client → broker.
type MessagePublisher struct {
	Head    Headers         `json:"headers"`
	PayLoad json.RawMessage `json:"payload"`
}

// FullHeaders includes the MessageId assigned by the broker.
type FullHeaders struct {
	MessageId uuid.UUID `json:"message_id"`
	Method    string    `json:"method"`
	Issuer    string    `json:"issuer"`
	Exchange  string    `json:"exchange"`
	Routing   string    `json:"routing"`
	ChannelID int       `json:"channel_id"`
	QueueName string    `json:"queuename"`
	Timestamp time.Time `json:"timestamp"`
}

// MessageConsumer is the frame delivered from broker → consumer.
type MessageConsumer struct {
	Head    FullHeaders     `json:"headers"`
	PayLoad json.RawMessage `json:"payload"`
	Broker  *Broker         `json:"-"`
}

// -----------------------------------------------------------------------
// Broker connection
// -----------------------------------------------------------------------

type Broker struct {
	port       string
	address    string
	connection net.Conn
	mu         sync.Mutex // protects writes
	nextChanID atomic.Int32
}

func ConnectBroker(address string, port string) (*Broker, error) {
	conn, err := net.Dial("tcp", address+":"+port)
	if err != nil {
		return nil, err
	}
	b := &Broker{
		port:       port,
		address:    address,
		connection: conn,
	}
	b.nextChanID.Store(1)
	return b, nil
}

// OpenChannel returns a new Channel scoped to this broker connection.
func (b *Broker) OpenChannel() *Channel {
	id := int(b.nextChanID.Add(1) - 1)
	return &Channel{
		ID:     id,
		broker: b,
	}
}

// -----------------------------------------------------------------------
// Channel – scoped operations
// -----------------------------------------------------------------------

// Channel is a logical multiplexed path over a single TCP connection.
type Channel struct {
	ID     int
	broker *Broker
}

// DeclareQueue tells the broker to create a queue.
func (ch *Channel) DeclareQueue(name string, size int) error {
	payload, _ := json.Marshal(map[string]interface{}{
		"name": name,
		"size": size,
	})
	return ch.send("DECLARE_QUEUE", "", "", payload)
}

// DeclareExchange tells the broker to create an exchange.
// kind is one of: "direct", "fanout", "topic".
func (ch *Channel) DeclareExchange(name string, kind string) error {
	payload, _ := json.Marshal(map[string]interface{}{
		"name": name,
		"type": kind,
	})
	return ch.send("DECLARE_EXCHANGE", name, "", payload)
}

// BindQueue binds a queue to an exchange with the given routing key.
func (ch *Channel) BindQueue(queueName, exchangeName, routingKey string) error {
	payload, _ := json.Marshal(map[string]interface{}{
		"queue_name":  queueName,
		"exchange":    exchangeName,
		"routing_key": routingKey,
	})
	return ch.send("BIND_QUEUE", exchangeName, routingKey, payload)
}

// Publish sends a message through an exchange with a routing key.
//
// For point-to-point (direct to queue), use:
//
//	ch.Publish("", queueName, issuer, message)   // default exchange
//
// For fanout:
//
//	ch.Publish("logs_exchange", "", issuer, message)
//
// For topic:
//
//	ch.Publish("events", "payments.due", issuer, message)
func (ch *Channel) Publish(ctx context.Context, exchangeName, routingKey, issuer string, message interface{}) error {
	payloadBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return ch.send("PUBLISH", exchangeName, routingKey, json.RawMessage(payloadBytes))
}

// Consume registers a consumer on a queue and returns a channel of messages.
func (ch *Channel) Consume(queueName, consumerTag string, autoAck bool) (<-chan MessageConsumer, error) {
	payload, _ := json.Marshal(map[string]interface{}{
		"autoack":    autoAck,
		"queue_name": queueName,
	})

	msg := MessagePublisher{
		Head: Headers{
			Method:    "CONSUME",
			Issuer:    consumerTag,
			Exchange:  "",
			Routing:   queueName,
			ChannelID: ch.ID,
			Timestamp: time.Now(),
		},
		PayLoad: json.RawMessage(payload),
	}

	ch.broker.mu.Lock()
	encoder := json.NewEncoder(ch.broker.connection)
	err := encoder.Encode(msg)
	ch.broker.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("send error: %w", err)
	}

	out := make(chan MessageConsumer)
	reader := bufio.NewReader(ch.broker.connection)
	go func() {
		defer close(out)
		for {
			data, err := reader.ReadBytes('\n')
			if err != nil {
				fmt.Println("Connection closed by server")
				return
			}
			var message MessageConsumer
			if err := json.Unmarshal(data, &message); err != nil {
				fmt.Printf("Error unmarshalling JSON: %v\n", err)
				continue
			}
			message.Broker = ch.broker
			out <- message
		}
	}()

	return out, nil
}

// send is the internal helper to write a framed message to the broker.
func (ch *Channel) send(method, exchangeName, routingKey string, payload json.RawMessage) error {
	msg := MessagePublisher{
		Head: Headers{
			Method:    method,
			Issuer:    "",
			Exchange:  exchangeName,
			Routing:   routingKey,
			ChannelID: ch.ID,
			Timestamp: time.Now(),
		},
		PayLoad: payload,
	}

	fullBytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	fullBytes = append(fullBytes, '\n')

	ch.broker.mu.Lock()
	_, err = ch.broker.connection.Write(fullBytes)
	ch.broker.mu.Unlock()
	return err
}

// -----------------------------------------------------------------------
// Message helpers
// -----------------------------------------------------------------------

// Ack acknowledges a consumed message.
func (m *MessageConsumer) Ack() error {
	messageID, err := json.Marshal(m.Head.MessageId)
	if err != nil {
		return err
	}

	msg := MessagePublisher{
		Head: Headers{
			Method:    "ACK",
			Issuer:    "",
			Exchange:  "",
			Routing:   m.Head.QueueName,
			ChannelID: m.Head.ChannelID,
			Timestamp: time.Now(),
		},
		PayLoad: json.RawMessage(messageID),
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	data = append(data, '\n')

	m.Broker.mu.Lock()
	_, err = m.Broker.connection.Write(data)
	m.Broker.mu.Unlock()
	return err
}

// Nack negatively acknowledges a consumed message (requeue).
func (m *MessageConsumer) Nack() error {
	messageID, err := json.Marshal(m.Head.MessageId)
	if err != nil {
		return err
	}

	msg := MessagePublisher{
		Head: Headers{
			Method:    "NACK",
			Issuer:    "",
			Exchange:  "",
			Routing:   m.Head.QueueName,
			ChannelID: m.Head.ChannelID,
			Timestamp: time.Now(),
		},
		PayLoad: json.RawMessage(messageID),
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	data = append(data, '\n')

	m.Broker.mu.Lock()
	_, err = m.Broker.connection.Write(data)
	m.Broker.mu.Unlock()
	return err
}

func GetBytes(key any) ([]byte, error) {
	return json.Marshal(key)
}
