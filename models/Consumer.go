package models

type Consumer struct {
	QueueName   string
	ConsumerTag string
	ChannelID   int
	AutoAck     bool
}
