package queues

import (
	"container/list"
	"encoding/json"
	"fmt"
	"net"
	"sync"

	"github.com/Max20050/go_message_broker/models"
	"github.com/google/uuid"
)

type Config struct {
	data_persist bool // TODO
}

type Queue struct { // Data structure for in-memory messages
	Id       uuid.UUID
	Name     string
	mu       *sync.Mutex                        // lock for race conditions
	Channel  chan models.StoredMessage          // Primary queue stream limited size
	overflow *list.List                         // overflow unlimited size but less performative
	InFlight map[uuid.UUID]models.StoredMessage // in-flight messages waiting to be acked or nacked and requeued
}

func CreateQueue(name string, queueSize int) Queue {
	id := uuid.New()
	return Queue{
		Id:       id,
		Name:     name,
		Channel:  make(chan models.StoredMessage, queueSize),
		overflow: list.New(),
		mu:       new(sync.Mutex),
		InFlight: make(map[uuid.UUID]models.StoredMessage),
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

// After a sub is registered as a consumer we start a ""worker"" who will dispatch the queued messages automatically.
func (s *Queue) StartDispacher(conn net.Conn) {
	for {
		if len(s.Channel) > 0 {
			msg := s.Dequeue()
			fmt.Println(msg)
			s.InFlight[msg.Head.MessageId] = msg
			encoder := json.NewEncoder(conn)
			err := encoder.Encode(msg)
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
