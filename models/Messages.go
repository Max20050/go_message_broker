package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type RecievedHeaders struct {
	Method    string    `json:"method"` // Publish/Consume
	Issuer    string    `json:"issuer"` //e.g: Backend
	QueueName string    `json:"queuename"`
	Context   string    `json:"context"`   // optional topic. e.g: Emails,messages
	Timestamp time.Time `json:"timestamp"` // Time
}

type RecievedMessage struct {
	Head    RecievedHeaders `json:"headers"`
	PayLoad json.RawMessage `json:"payload"`
}

type Headers struct {
	MessageId uuid.UUID `json:"message_id"`
	Method    string    `json:"method"` // Publish/Consume
	Issuer    string    `json:"issuer"` //e.g: Backend
	QueueName string    `json:"queuename"`
	Context   string    `json:"context"`   // optional topic. e.g: Emails,messages
	Timestamp time.Time `json:"timestamp"` // Time
}

type StoredMessage struct {
	Head    Headers         `json:"headers"`
	PayLoad json.RawMessage `json:"payload"`
}

func (m *RecievedMessage) ToStorage() StoredMessage {
	return StoredMessage{
		Head: Headers{
			Method:    m.Head.Method,
			Issuer:    m.Head.Issuer,
			QueueName: m.Head.QueueName,
			Context:   m.Head.Context,
			Timestamp: m.Head.Timestamp,
		},
		PayLoad: m.PayLoad,
	}
}
