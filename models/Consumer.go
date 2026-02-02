package models

type Consumer struct {
	QueueName   string
	ConsumerTag string
	AutoAck     bool
}

type ConsumerPayload struct {
	AutoAck bool `json:"autoack"`
}
