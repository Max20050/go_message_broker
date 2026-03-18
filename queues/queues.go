package queues

import (
	"container/list"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

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

// EnqueueDirect adds a message without generating a new ID.
// Used by the persistence layer to restore messages with their original IDs.
func (q *Queue) EnqueueDirect(m models.StoredMessage) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.Channel) == cap(q.Channel) {
		q.overflow.PushBack(m)
	} else {
		q.Channel <- m
	}
}

// Capacity returns the buffer size of the queue channel.
func (q *Queue) Capacity() int {
	return cap(q.Channel)
}

// Dequeue blocks until a message is available and returns it.
func (q *Queue) Dequeue() (models.StoredMessage, bool) {
	msg, ok := <-q.Channel
	return msg, ok
}

// StartDispatcher dispatches queued messages to a consumer connection.
// It blocks on the channel receive — no busy-loop, no heartbeat read.
func (q *Queue) StartDispatcher(conn net.Conn, consumerTag string) {
	encoder := json.NewEncoder(conn)

	for msg := range q.Channel {
		fmt.Printf("[dispatcher] delivering msg %s to consumer %q\n", msg.Head.MessageId, consumerTag)

		consumer, exists := q.Consumers[consumerTag]
		if !exists {
			// Consumer was removed — requeue the message and stop.
			q.Enqueue(msg)
			fmt.Printf("[dispatcher] consumer %q gone, stopping\n", consumerTag)
			return
		}

		if !consumer.AutoAck {
			q.mu.Lock()
			q.InFlight[msg.Head.MessageId] = msg
			q.mu.Unlock()
		}

		// Use a write deadline to detect dead connections instead of
		// a blocking conn.Read() heartbeat (which caused a deadlock).
		conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err := encoder.Encode(msg); err != nil {
			fmt.Println("[dispatcher] send error:", err)
			// Requeue the message so it isn't lost.
			q.Enqueue(msg)
			return
		}
		conn.SetWriteDeadline(time.Time{}) // clear deadline
	}
}

func (q *Queue) HandleAck(messageID uuid.UUID) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, exist := q.InFlight[messageID]; !exist {
		return fmt.Errorf("ERROR: No message to ack with the provided id")
	}
	delete(q.InFlight, messageID)
	fmt.Println("MESSAGE ACKED SUCCESSFULLY")
	return nil
}

func (q *Queue) HandleNack(messageID uuid.UUID) error {
	q.mu.Lock()
	msg, exist := q.InFlight[messageID]
	if !exist {
		q.mu.Unlock()
		return fmt.Errorf("ERROR: No message to nack with the provided id")
	}
	delete(q.InFlight, messageID)
	q.mu.Unlock()

	q.Enqueue(msg) // requeue the message
	return nil
}

// ListInFlight returns a copy of messages currently in-flight (awaiting ACK).
func (q *Queue) ListInFlight() []models.StoredMessage {
	q.mu.Lock()
	defer q.mu.Unlock()
	msgs := make([]models.StoredMessage, 0, len(q.InFlight))
	for _, m := range q.InFlight {
		msgs = append(msgs, m)
	}
	return msgs
}

// PeekMessages returns a snapshot of both buffered (queued) and in-flight messages.
// Buffered messages are drained and immediately re-enqueued so nothing is lost.
func (q *Queue) PeekMessages() (buffered []models.StoredMessage, inflight []models.StoredMessage) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Snapshot in-flight
	inflight = make([]models.StoredMessage, 0, len(q.InFlight))
	for _, m := range q.InFlight {
		inflight = append(inflight, m)
	}

	// Drain the channel buffer to peek, then put them all back
	n := len(q.Channel)
	buffered = make([]models.StoredMessage, 0, n)
	for i := 0; i < n; i++ {
		select {
		case msg := <-q.Channel:
			buffered = append(buffered, msg)
		default:
			break
		}
	}
	// Re-enqueue all drained messages (same order)
	for _, msg := range buffered {
		q.Channel <- msg
	}

	return buffered, inflight
}

// RemoveFromBuffer removes a single message from the channel buffer by its ID.
// It drains the channel, skips the target, and re-enqueues the rest.
// Returns true if the message was found and removed.
func (q *Queue) RemoveFromBuffer(messageID uuid.UUID) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	n := len(q.Channel)
	found := false
	kept := make([]models.StoredMessage, 0, n)

	for i := 0; i < n; i++ {
		select {
		case msg := <-q.Channel:
			if !found && msg.Head.MessageId == messageID {
				found = true // drop this message
				fmt.Printf("MESSAGE REMOVED FROM BUFFER: %s\n", messageID)
			} else {
				kept = append(kept, msg)
			}
		default:
		}
	}

	// Re-enqueue the remaining messages
	for _, msg := range kept {
		q.Channel <- msg
	}

	return found
}
