package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// RecievedHeaders are the headers as they arrive from the client over the wire.
type RecievedHeaders struct {
	Method    string    `json:"method"`    // PUBLISH, CONSUME, ACK, NACK, DECLARE_QUEUE, DECLARE_EXCHANGE, BIND_QUEUE
	Issuer    string    `json:"issuer"`    // e.g: "Backend"
	Exchange  string    `json:"exchange"`  // Exchange name ("" = default direct)
	Routing   string    `json:"routing"`   // Routing key (queue name for direct, topic pattern, etc.)
	ChannelID int       `json:"channel_id"`
	Timestamp time.Time `json:"timestamp"`
}

// RecievedMessage is the raw message received from the wire.
type RecievedMessage struct {
	Head    RecievedHeaders `json:"headers"`
	PayLoad json.RawMessage `json:"payload"`
}

// Headers are the enriched headers stored inside the broker.
type Headers struct {
	MessageId uuid.UUID `json:"message_id"`
	Method    string    `json:"method"`
	Issuer    string    `json:"issuer"`
	Exchange  string    `json:"exchange"`
	Routing   string    `json:"routing"`
	ChannelID int       `json:"channel_id"`
	QueueName string    `json:"queuename"` // Set by the exchange when routing
	Timestamp time.Time `json:"timestamp"`
}

// StoredMessage is the message as stored inside a queue.
type StoredMessage struct {
	Head    Headers         `json:"headers"`
	PayLoad json.RawMessage `json:"payload"`
}

// ToStorage converts a received message into storage format.
func (m *RecievedMessage) ToStorage() StoredMessage {
	return StoredMessage{
		Head: Headers{
			Method:    m.Head.Method,
			Issuer:    m.Head.Issuer,
			Exchange:  m.Head.Exchange,
			Routing:   m.Head.Routing,
			ChannelID: m.Head.ChannelID,
			Timestamp: m.Head.Timestamp,
		},
		PayLoad: m.PayLoad,
	}
}

// --- Payloads for control-plane operations ---

// DeclareQueuePayload is sent by the client to declare (create) a queue.
type DeclareQueuePayload struct {
	Name string `json:"name"`
	Size int    `json:"size"` // 0 = default (1000)
}

// DeclareExchangePayload is sent by the client to declare (create) an exchange.
type DeclareExchangePayload struct {
	Name string `json:"name"` // Exchange name
	Type string `json:"type"` // "direct", "fanout", "topic"
}

// BindQueuePayload is sent by the client to bind a queue to an exchange.
type BindQueuePayload struct {
	QueueName  string `json:"queue_name"`
	Exchange   string `json:"exchange"`
	RoutingKey string `json:"routing_key"` // For direct/topic; ignored by fanout
}

// ConsumerPayload carries configuration for consuming.
type ConsumerPayload struct {
	AutoAck   bool   `json:"autoack"`
	QueueName string `json:"queue_name"`
}

// AuthPayload is sent by the client as the first message to authenticate.
type AuthPayload struct {
	Username string `json:"username"`
	Password string `json:"password"`
}
