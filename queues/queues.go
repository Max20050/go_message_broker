package queues

import (
	"container/list"
	"encoding/json"
	"fmt"
	"net"
	"sync"

	"github.com/google/uuid"
)

type Message struct {
	Context string
	Payload string
}

type Queue struct { // Data structure for in-memory messages
	Id       uuid.UUID
	Name     string
	mu       *sync.Mutex  // lock for race conditions
	Channel  chan Message // Primary queue stream limited size
	overflow *list.List   // overflow unlimited size but less performative
}

func CreateQueue(name string, queueSize int) Queue {
	id := uuid.New()
	return Queue{
		Id:       id,
		Name:     name,
		Channel:  make(chan Message, queueSize),
		overflow: list.New(),
		mu:       new(sync.Mutex),
	}
}

func (q *Queue) Enqueue(m Message) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.Channel) == cap(q.Channel) {
		q.overflow.PushBack(m)
	} else {
		q.Channel <- m
	}
}

func (q *Queue) Dequeue() Message {
	q.mu.Lock()
	m := <-q.Channel
	q.mu.Unlock()
	return m
}

func (s *Queue) StartDispacher(conn net.Conn) {
	for {
		if len(s.Channel) > 0 {
			msg := s.Dequeue()
			fmt.Println(msg)
			encoder := json.NewEncoder(conn)
			err := encoder.Encode(msg)
			if err != nil {
				fmt.Println("Send error:", err)
				return
			}
		}
	}
}
