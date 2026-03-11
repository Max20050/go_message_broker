package queues

import (
	"container/list"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/Max20050/go_message_broker/models"
	"github.com/google/uuid"
)

type Config struct {
	DataPersist bool // TODO
}

type Queue struct { // Data structure for in-memory messages
	Id        uuid.UUID
	Name      string
	mu        *sync.Mutex
	Channel   chan models.StoredMessage           // Primary queue stream (limited size)
	overflow  *list.List                          // Overflow (unlimited size but less performant)
	InFlight  map[uuid.UUID]models.StoredMessage  // In-flight messages waiting to be acked/nacked
	Consumers map[string]models.Consumer          // Registered consumers
}

func CreateQueue(name string, queueSize int) Queue {
	if queueSize <= 0 {
		queueSize = 1000
	}
	id := uuid.New()
	return Queue{
		Id:        id,
		Name:      name,
		Channel:   make(chan models.StoredMessage, queueSize),
		overflow:  list.New(),
		mu:        new(sync.Mutex),
		InFlight:  make(map[uuid.UUID]models.StoredMessage),
		Consumers: make(map[string]models.Consumer),
	}
}

func (q *Queue) ToInflight(msg models.StoredMessage) {
	q.InFlight[msg.Head.MessageId] = msg
}

func (q *Queue) Enqueue(m models.StoredMessage) {
	q.mu.Lock()
	defer q.mu.Unlock()

	mId := uuid.New()
	m.Head.MessageId = mId
	if len(q.Channel) == cap(q.Channel) {
		q.overflow.PushBack(m)
	} else {
		q.Channel <- m
	}
}

func (q *Queue) Dequeue() models.StoredMessage {
	q.mu.Lock()
	m := <-q.Channel
	q.mu.Unlock()
	return m
}

// StartDispatcher dispatches queued messages to a consumer connection.
func (q *Queue) StartDispatcher(conn net.Conn, consumerTag string) {
	for {
		if len(q.Channel) > 0 {
			err := ConsumerHeartBeat(conn)
			if err != nil {
				return
			}

			msg := q.Dequeue()
			fmt.Println(msg)

			consumer, exists := q.Consumers[consumerTag]
			if !exists {
				return // consumer was removed
			}

			if !consumer.AutoAck {
				q.InFlight[msg.Head.MessageId] = msg
			}

			encoder := json.NewEncoder(conn)
			err = encoder.Encode(msg)
			if err != nil {
				fmt.Println("Send error:", err)
				return
			}
		}
	}
}

func (q *Queue) HandleAck(messageID uuid.UUID) error {
	if _, exist := q.InFlight[messageID]; !exist {
		return fmt.Errorf("ERROR: No message to ack with the provided id")
	}
	delete(q.InFlight, messageID)
	_, exist := q.InFlight[messageID]
	if !exist {
		fmt.Println("MESSAGE ACKED SUCCESSFULLY")
		return nil
	}
	return nil // !ADD ERROR
}

func (q *Queue) HandleNack(messageID uuid.UUID) error {
	m, exist := q.InFlight[messageID]
	if !exist {
		return fmt.Errorf("ERROR: No message to nack with the provided id")
	}
	q.Enqueue(m) // requeue the message
	delete(q.InFlight, messageID)
	return nil
}

func ConsumerHeartBeat(conn net.Conn) error {
	buffer := make([]byte, 1024)
	_, err := conn.Read(buffer)
	if err == io.EOF {
		fmt.Printf("Connection closed by remote peer: %s\n", conn.RemoteAddr())
		return err // Exit the handler goroutine
	}
	return nil
}
