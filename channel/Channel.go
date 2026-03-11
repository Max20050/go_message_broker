package channel

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
)

// Channel is a logical multiplexed connection over a single TCP connection.
// Clients open one or more channels on a connection; each channel scopes
// operations (publish, consume, ack/nack) independently.
type Channel struct {
	ID     int
	conn   net.Conn
	closed atomic.Bool
}

// Manager tracks all open channels for a single TCP connection.
type Manager struct {
	mu       sync.RWMutex
	channels map[int]*Channel
	nextID   int
	conn     net.Conn
}

// NewManager creates a channel manager for the given connection.
func NewManager(conn net.Conn) *Manager {
	return &Manager{
		channels: make(map[int]*Channel),
		nextID:   1,
		conn:     conn,
	}
}

// Open creates a new channel and returns it.
func (m *Manager) Open() *Channel {
	m.mu.Lock()
	defer m.mu.Unlock()

	ch := &Channel{
		ID:   m.nextID,
		conn: m.conn,
	}
	m.channels[ch.ID] = ch
	m.nextID++
	return ch
}

// Get returns an existing channel by ID, creating channel 0 (default)
// implicitly if it doesn't exist.
func (m *Manager) Get(id int) (*Channel, error) {
	m.mu.RLock()
	ch, ok := m.channels[id]
	m.mu.RUnlock()
	if ok {
		return ch, nil
	}

	// Auto-create if the client refers to it for the first time.
	m.mu.Lock()
	defer m.mu.Unlock()
	// Double-check after acquiring write lock.
	if ch, ok = m.channels[id]; ok {
		return ch, nil
	}
	ch = &Channel{
		ID:   id,
		conn: m.conn,
	}
	m.channels[id] = ch
	return ch, nil
}

// Close shuts down a specific channel by ID.
func (m *Manager) Close(id int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	ch, ok := m.channels[id]
	if !ok {
		return fmt.Errorf("channel %d does not exist", id)
	}
	ch.closed.Store(true)
	delete(m.channels, id)
	return nil
}

// CloseAll shuts down all channels (called when the connection drops).
func (m *Manager) CloseAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, ch := range m.channels {
		ch.closed.Store(true)
		delete(m.channels, id)
	}
}

// IsClosed reports whether the channel was closed.
func (c *Channel) IsClosed() bool {
	return c.closed.Load()
}
