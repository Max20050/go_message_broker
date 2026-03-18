package persistence

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
)

// ── Snapshot types ─────────────────────────────────────────────────────

// BrokerSnapshot captures the entire broker state for persistence.
type BrokerSnapshot struct {
	Version   int                `json:"version"`
	Timestamp time.Time          `json:"timestamp"`
	Exchanges []ExchangeSnapshot `json:"exchanges"`
	Queues    []QueueSnapshot    `json:"queues"`
}

// ExchangeSnapshot captures an exchange's definition and bindings.
type ExchangeSnapshot struct {
	Name     string            `json:"name"`
	Type     string            `json:"type"`
	Bindings []BindingSnapshot `json:"bindings"`
}

// BindingSnapshot captures a single routing-key → queue binding.
type BindingSnapshot struct {
	RoutingKey string `json:"routing_key"`
	QueueName  string `json:"queue_name"`
}

// QueueSnapshot captures a queue's definition and all its messages.
type QueueSnapshot struct {
	Name     string            `json:"name"`
	Size     int               `json:"size"`
	Messages []MessageSnapshot `json:"messages"`
}

// MessageSnapshot captures a single message stored in a queue.
type MessageSnapshot struct {
	MessageId string          `json:"message_id"`
	Method    string          `json:"method"`
	Issuer    string          `json:"issuer"`
	Exchange  string          `json:"exchange"`
	Routing   string          `json:"routing"`
	ChannelID int             `json:"channel_id"`
	QueueName string          `json:"queuename"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

// ── FileStore ──────────────────────────────────────────────────────────

// FileStore persists broker state to a JSON file on disk.
type FileStore struct {
	dir      string
	filename string
}

// NewFileStore creates a file-based persistence store.
// It ensures the data directory exists.
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create persistence dir %q: %w", dir, err)
	}
	return &FileStore{
		dir:      dir,
		filename: filepath.Join(dir, "broker_state.json"),
	}, nil
}

// Save writes the broker snapshot to disk atomically.
// It writes to a temp file first, then renames it to avoid corruption.
func (fs *FileStore) Save(snap BrokerSnapshot) error {
	snap.Version = 1
	snap.Timestamp = time.Now()

	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}

	tmpFile := fs.filename + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return fmt.Errorf("write temp file: %w", err)
	}

	if err := os.Rename(tmpFile, fs.filename); err != nil {
		return fmt.Errorf("rename temp file: %w", err)
	}

	return nil
}

// Load reads a broker snapshot from disk.
// Returns an empty snapshot if the file doesn't exist (fresh start).
func (fs *FileStore) Load() (BrokerSnapshot, error) {
	data, err := os.ReadFile(fs.filename)
	if err != nil {
		if os.IsNotExist(err) {
			return BrokerSnapshot{}, nil // fresh start
		}
		return BrokerSnapshot{}, fmt.Errorf("read state file: %w", err)
	}

	var snap BrokerSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return BrokerSnapshot{}, fmt.Errorf("unmarshal state file: %w", err)
	}

	return snap, nil
}

// FilePath returns the path to the state file.
func (fs *FileStore) FilePath() string {
	return fs.filename
}

// ── Helper: convert uuid.UUID to/from string for snapshots ─────────

func ParseUUID(s string) (uuid.UUID, error) {
	return uuid.Parse(s)
}
