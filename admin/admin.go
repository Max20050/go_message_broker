package admin

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	exchange "github.com/Max20050/go_message_broker/Exchange"
	"github.com/Max20050/go_message_broker/queues"
	"github.com/google/uuid"
)

// Server holds references to broker state and exposes an HTTP admin panel.
type Server struct {
	Port      string
	Queues    func() map[string]*queues.Queue // read-only accessor
	Exchanges *exchange.Registry
}

// QueueInfo is the JSON representation of a queue for the admin API.
type QueueInfo struct {
	ID            string         `json:"id"`
	Name          string         `json:"name"`
	MessageCount  int            `json:"message_count"`
	Capacity      int            `json:"capacity"`
	InFlightCount int            `json:"inflight_count"`
	ConsumerCount int            `json:"consumer_count"`
	Consumers     []ConsumerInfo `json:"consumers"`
}

// ConsumerInfo is the JSON representation of a consumer.
type ConsumerInfo struct {
	ConsumerTag string `json:"consumer_tag"`
	QueueName   string `json:"queue_name"`
	ChannelID   int    `json:"channel_id"`
	AutoAck     bool   `json:"auto_ack"`
}

// OverviewInfo is the JSON representation of the general overview.
type OverviewInfo struct {
	TotalQueues    int `json:"total_queues"`
	TotalExchanges int `json:"total_exchanges"`
	TotalConsumers int `json:"total_consumers"`
	TotalMessages  int `json:"total_messages"`
}

// MessageInfo is the JSON representation of a message for the admin API.
type MessageInfo struct {
	MessageID string          `json:"message_id"`
	Method    string          `json:"method"`
	Issuer    string          `json:"issuer"`
	Exchange  string          `json:"exchange"`
	Routing   string          `json:"routing"`
	QueueName string          `json:"queue_name"`
	Timestamp string          `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
	Status    string          `json:"status"` // "queued" or "inflight"
}

// Start launches the admin HTTP server on the specified port.
func (s *Server) Start(wg *sync.WaitGroup) {
	mux := http.NewServeMux()

	// API endpoints ─────────────────────────────────────────────
	mux.HandleFunc("/api/overview", s.handleOverview)
	mux.HandleFunc("/api/queues", s.handleQueues)
	mux.HandleFunc("/api/exchanges", s.handleExchanges)
	mux.HandleFunc("/api/consumers", s.handleConsumers)
	mux.HandleFunc("/api/messages", s.handleMessages)
	mux.HandleFunc("/api/ack", s.handleAdminAck)

	// Dashboard (single-page HTML) ─────────────────────────────
	mux.HandleFunc("/", s.handleDashboard)

	addr := ":" + s.Port
	fmt.Printf("Admin panel running on http://localhost%s\n", addr)

	if wg != nil {
		wg.Done() // signal that we started
	}

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Admin server error: %v", err)
	}
}

// ── API handlers ──────────────────────────────────────────────────────

func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	qMap := s.Queues()

	totalConsumers := 0
	totalMessages := 0
	for _, q := range qMap {
		totalConsumers += len(q.Consumers)
		totalMessages += len(q.Channel) + len(q.InFlight)
	}

	overview := OverviewInfo{
		TotalQueues:    len(qMap),
		TotalExchanges: len(s.Exchanges.ListAll()),
		TotalConsumers: totalConsumers,
		TotalMessages:  totalMessages,
	}
	writeJSON(w, overview)
}

func (s *Server) handleQueues(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	qMap := s.Queues()

	result := make([]QueueInfo, 0, len(qMap))
	for _, q := range qMap {
		consumers := make([]ConsumerInfo, 0, len(q.Consumers))
		for _, c := range q.Consumers {
			consumers = append(consumers, ConsumerInfo{
				ConsumerTag: c.ConsumerTag,
				QueueName:   c.QueueName,
				ChannelID:   c.ChannelID,
				AutoAck:     c.AutoAck,
			})
		}
		result = append(result, QueueInfo{
			ID:            q.Id.String(),
			Name:          q.Name,
			MessageCount:  len(q.Channel),
			Capacity:      cap(q.Channel),
			InFlightCount: len(q.InFlight),
			ConsumerCount: len(q.Consumers),
			Consumers:     consumers,
		})
	}
	writeJSON(w, result)
}

func (s *Server) handleExchanges(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	writeJSON(w, s.Exchanges.ListAll())
}

func (s *Server) handleConsumers(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	qMap := s.Queues()

	var result []ConsumerInfo
	for _, q := range qMap {
		for _, c := range q.Consumers {
			result = append(result, ConsumerInfo{
				ConsumerTag: c.ConsumerTag,
				QueueName:   c.QueueName,
				ChannelID:   c.ChannelID,
				AutoAck:     c.AutoAck,
			})
		}
	}
	if result == nil {
		result = []ConsumerInfo{}
	}
	writeJSON(w, result)
}

// ── Messages handler ──────────────────────────────────────────────────

func (s *Server) handleMessages(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	queueName := r.URL.Query().Get("queue")
	if queueName == "" {
		http.Error(w, `{"error":"missing ?queue= parameter"}`, http.StatusBadRequest)
		return
	}

	qMap := s.Queues()
	q, exists := qMap[queueName]
	if !exists {
		http.Error(w, `{"error":"queue not found"}`, http.StatusNotFound)
		return
	}

	buffered, inflight := q.PeekMessages()

	result := make([]MessageInfo, 0, len(buffered)+len(inflight))
	for _, m := range buffered {
		result = append(result, MessageInfo{
			MessageID: m.Head.MessageId.String(),
			Method:    m.Head.Method,
			Issuer:    m.Head.Issuer,
			Exchange:  m.Head.Exchange,
			Routing:   m.Head.Routing,
			QueueName: m.Head.QueueName,
			Timestamp: m.Head.Timestamp.Format("2006-01-02 15:04:05"),
			Payload:   m.PayLoad,
			Status:    "queued",
		})
	}
	for _, m := range inflight {
		result = append(result, MessageInfo{
			MessageID: m.Head.MessageId.String(),
			Method:    m.Head.Method,
			Issuer:    m.Head.Issuer,
			Exchange:  m.Head.Exchange,
			Routing:   m.Head.Routing,
			QueueName: m.Head.QueueName,
			Timestamp: m.Head.Timestamp.Format("2006-01-02 15:04:05"),
			Payload:   m.PayLoad,
			Status:    "inflight",
		})
	}
	writeJSON(w, result)
}

// ── ACK handler (admin) ───────────────────────────────────────────────

type ackRequest struct {
	QueueName string `json:"queue_name"`
	MessageID string `json:"message_id"`
}

func (s *Server) handleAdminAck(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	var req ackRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	qMap := s.Queues()
	q, exists := qMap[req.QueueName]
	if !exists {
		http.Error(w, `{"error":"queue not found"}`, http.StatusNotFound)
		return
	}

	msgID, err := uuid.Parse(req.MessageID)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"invalid message id: %s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	// Try in-flight first, then fall back to removing from buffer.
	if err := q.HandleAck(msgID); err == nil {
		writeJSON(w, map[string]string{"status": "ok", "message": "in-flight message acked"})
		return
	}

	if q.RemoveFromBuffer(msgID) {
		writeJSON(w, map[string]string{"status": "ok", "message": "queued message removed"})
		return
	}

	http.Error(w, `{"error":"message not found in queue"}`, http.StatusNotFound)
}

// ── Dashboard HTML ────────────────────────────────────────────────────

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(dashboardHTML))
}

// ── Helpers ───────────────────────────────────────────────────────────

func setCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(data)
}
