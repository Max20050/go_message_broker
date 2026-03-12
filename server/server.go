package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/Max20050/go_message_broker/channel"
	exchange "github.com/Max20050/go_message_broker/Exchange"
	"github.com/Max20050/go_message_broker/models"
	"github.com/Max20050/go_message_broker/queues"
	"github.com/google/uuid"
)

type Server struct {
	Port       string
	Listener   net.Listener
	mu         sync.RWMutex
	Queues     map[string]*queues.Queue
	Exchanges  *exchange.Registry
}

func CreteTcpServer(port string) (Server, error) {
	listener, err := net.Listen("tcp", "localhost:"+port)
	if err != nil {
		return Server{}, err
	}

	fmt.Println("Server Listening on:", port)

	return Server{
		Port:      port,
		Listener:  listener,
		Queues:    make(map[string]*queues.Queue),
		Exchanges: exchange.NewRegistry(),
	}, nil
}

// GetMu returns a pointer to the server's read-write mutex.
func (s *Server) GetMu() *sync.RWMutex {
	return &s.mu
}

// Accept accepts incoming TCP connections and spawns a handler goroutine for each.
func (s *Server) Accept() error {
	for {
		conn, err := s.Listener.Accept()
		if err != nil {
			log.Println("Error accepting connection:", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

// hardcoded credentials (temporary)
var validUsers = map[string]string{
	"root": "root",
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	fmt.Println("New connection:", conn.RemoteAddr().String())

	// ---- Authentication handshake ----
	if !s.authenticate(conn, scanner) {
		return // connection already closed with error
	}

	// ---- Authenticated – proceed normally ----
	chManager := channel.NewManager(conn)
	defer chManager.CloseAll()

	for scanner.Scan() {
		jsonData := scanner.Bytes()

		fmt.Printf("Raw JSON: %s\n", string(jsonData))

		var msg models.RecievedMessage
		if err := json.Unmarshal(jsonData, &msg); err != nil {
			fmt.Printf("Error unmarshalling JSON: %v\n", err)
			continue
		}

		// Ensure the channel exists for this message.
		_, err := chManager.Get(msg.Head.ChannelID)
		if err != nil {
			fmt.Printf("Channel error: %v\n", err)
			continue
		}

		switch msg.Head.Method {
		case "DECLARE_QUEUE":
			s.handleDeclareQueue(conn, msg)
		case "DECLARE_EXCHANGE":
			s.handleDeclareExchange(conn, msg)
		case "BIND_QUEUE":
			s.handleBindQueue(conn, msg)
		case "PUBLISH":
			s.handlePublish(conn, msg)
		case "CONSUME":
			s.handleConsume(conn, msg)
		case "ACK":
			s.handleAck(msg)
		case "NACK":
			s.handleNack(msg)
		default:
			fmt.Printf("Unknown method: %s\n", msg.Head.Method)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading from connection: %v\n", err)
	}
}

// authenticate reads the first frame from the connection and validates
// the credentials. Returns true if the client is authenticated.
func (s *Server) authenticate(conn net.Conn, scanner *bufio.Scanner) bool {
	if !scanner.Scan() {
		fmt.Printf("Auth failed: connection closed before auth from %s\n", conn.RemoteAddr())
		return false
	}

	jsonData := scanner.Bytes()
	var msg models.RecievedMessage
	if err := json.Unmarshal(jsonData, &msg); err != nil {
		sendError(conn, "AUTH", fmt.Sprintf("invalid auth frame: %v", err))
		fmt.Printf("Auth failed: bad JSON from %s\n", conn.RemoteAddr())
		return false
	}

	if msg.Head.Method != "AUTH" {
		sendError(conn, "AUTH", "first message must be AUTH")
		fmt.Printf("Auth failed: expected AUTH, got %q from %s\n", msg.Head.Method, conn.RemoteAddr())
		return false
	}

	var creds models.AuthPayload
	if err := json.Unmarshal(msg.PayLoad, &creds); err != nil {
		sendError(conn, "AUTH", fmt.Sprintf("bad auth payload: %v", err))
		fmt.Printf("Auth failed: bad payload from %s\n", conn.RemoteAddr())
		return false
	}

	expectedPassword, userExists := validUsers[creds.Username]
	if !userExists || expectedPassword != creds.Password {
		sendError(conn, "AUTH", "invalid credentials")
		fmt.Printf("Auth failed: invalid credentials (user=%q) from %s\n", creds.Username, conn.RemoteAddr())
		return false
	}

	sendOK(conn, "AUTH", fmt.Sprintf("welcome %s", creds.Username))
	fmt.Printf("Auth successful: user=%q from %s\n", creds.Username, conn.RemoteAddr())
	return true
}

// ---------------------------------------------------------------------------
// DECLARE_QUEUE
// ---------------------------------------------------------------------------

func (s *Server) handleDeclareQueue(conn net.Conn, msg models.RecievedMessage) {
	var payload models.DeclareQueuePayload
	if err := json.Unmarshal(msg.PayLoad, &payload); err != nil {
		sendError(conn, "DECLARE_QUEUE", fmt.Sprintf("bad payload: %v", err))
		return
	}

	s.mu.Lock()
	if _, exists := s.Queues[payload.Name]; !exists {
		q := queues.CreateQueue(payload.Name, payload.Size)
		s.Queues[payload.Name] = &q

		// Auto-bind to the default exchange (routing key = queue name).
		if defaultEx, ok := s.Exchanges.Get(""); ok {
			defaultEx.Bind(payload.Name, &q)
			// Re-assign pointer since CreateQueue returns a value.
			s.Queues[payload.Name] = &q
		}

		fmt.Printf("Queue declared: %s (size=%d)\n", payload.Name, payload.Size)
	}
	s.mu.Unlock()

	sendOK(conn, "DECLARE_QUEUE", fmt.Sprintf("queue %q ready", payload.Name))
}

// ---------------------------------------------------------------------------
// DECLARE_EXCHANGE
// ---------------------------------------------------------------------------

func (s *Server) handleDeclareExchange(conn net.Conn, msg models.RecievedMessage) {
	var payload models.DeclareExchangePayload
	if err := json.Unmarshal(msg.PayLoad, &payload); err != nil {
		sendError(conn, "DECLARE_EXCHANGE", fmt.Sprintf("bad payload: %v", err))
		return
	}

	_, err := s.Exchanges.Declare(payload.Name, payload.Type)
	if err != nil {
		sendError(conn, "DECLARE_EXCHANGE", err.Error())
		return
	}

	fmt.Printf("Exchange declared: %s (type=%s)\n", payload.Name, payload.Type)
	sendOK(conn, "DECLARE_EXCHANGE", fmt.Sprintf("exchange %q ready", payload.Name))
}

// ---------------------------------------------------------------------------
// BIND_QUEUE
// ---------------------------------------------------------------------------

func (s *Server) handleBindQueue(conn net.Conn, msg models.RecievedMessage) {
	var payload models.BindQueuePayload
	if err := json.Unmarshal(msg.PayLoad, &payload); err != nil {
		sendError(conn, "BIND_QUEUE", fmt.Sprintf("bad payload: %v", err))
		return
	}

	s.mu.RLock()
	q, qExists := s.Queues[payload.QueueName]
	s.mu.RUnlock()
	if !qExists {
		sendError(conn, "BIND_QUEUE", fmt.Sprintf("queue %q does not exist", payload.QueueName))
		return
	}

	ex, exExists := s.Exchanges.Get(payload.Exchange)
	if !exExists {
		sendError(conn, "BIND_QUEUE", fmt.Sprintf("exchange %q does not exist", payload.Exchange))
		return
	}

	ex.Bind(payload.RoutingKey, q)
	fmt.Printf("Queue %q bound to exchange %q with key %q\n", payload.QueueName, payload.Exchange, payload.RoutingKey)
	sendOK(conn, "BIND_QUEUE", "ok")
}

// ---------------------------------------------------------------------------
// PUBLISH – route through the exchange
// ---------------------------------------------------------------------------

func (s *Server) handlePublish(conn net.Conn, msg models.RecievedMessage) {
	exchangeName := msg.Head.Exchange
	routingKey := msg.Head.Routing

	ex, exists := s.Exchanges.Get(exchangeName)
	if !exists {
		// Fallback: if exchange is empty, use the default exchange with routing key = queue name.
		if exchangeName == "" {
			ex, _ = s.Exchanges.Get("")
		} else {
			sendError(conn, "PUBLISH", fmt.Sprintf("exchange %q does not exist", exchangeName))
			return
		}
	}

	// For the default exchange, auto-declare the queue if it doesn't exist.
	if exchangeName == "" {
		s.mu.Lock()
		if _, qExists := s.Queues[routingKey]; !qExists {
			q := queues.CreateQueue(routingKey, 1000)
			s.Queues[routingKey] = &q
			ex.Bind(routingKey, s.Queues[routingKey])
		}
		s.mu.Unlock()
	}

	stored := msg.ToStorage()
	if err := ex.Route(routingKey, stored); err != nil {
		fmt.Printf("Routing error: %v\n", err)
		sendError(conn, "PUBLISH", err.Error())
		return
	}

	fmt.Printf("Message published → exchange=%q routing=%q\n", exchangeName, routingKey)
}

// ---------------------------------------------------------------------------
// CONSUME
// ---------------------------------------------------------------------------

func (s *Server) handleConsume(conn net.Conn, msg models.RecievedMessage) {
	var payload models.ConsumerPayload
	if err := json.Unmarshal(msg.PayLoad, &payload); err != nil {
		sendError(conn, "CONSUME", fmt.Sprintf("bad payload: %v", err))
		return
	}

	queueName := payload.QueueName
	if queueName == "" {
		queueName = msg.Head.Routing // fallback to routing key
	}

	s.mu.RLock()
	q, exists := s.Queues[queueName]
	s.mu.RUnlock()
	if !exists {
		sendError(conn, "CONSUME", fmt.Sprintf("queue %q does not exist – declare it first", queueName))
		return
	}

	consumer := models.Consumer{
		QueueName:   queueName,
		ConsumerTag: msg.Head.Issuer,
		ChannelID:   msg.Head.ChannelID,
		AutoAck:     payload.AutoAck,
	}
	q.Consumers[consumer.ConsumerTag] = consumer

	fmt.Printf("Consumer %q registered on queue %q (channel %d)\n",
		consumer.ConsumerTag, queueName, consumer.ChannelID)

	go q.StartDispatcher(conn, consumer.ConsumerTag)
}

// ---------------------------------------------------------------------------
// ACK / NACK
// ---------------------------------------------------------------------------

func (s *Server) handleAck(msg models.RecievedMessage) {
	fmt.Println("ACK request")
	queueName := msg.Head.Routing
	s.mu.RLock()
	q, exists := s.Queues[queueName]
	s.mu.RUnlock()
	if !exists {
		fmt.Printf("ACK error: queue %q not found\n", queueName)
		return
	}

	var msgID uuid.UUID
	if err := msgID.UnmarshalText(msg.PayLoad); err != nil {
		fmt.Printf("ACK error: bad message id: %v\n", err)
		return
	}
	if err := q.HandleAck(msgID); err != nil {
		fmt.Printf("ACK error: %v\n", err)
	}
}

func (s *Server) handleNack(msg models.RecievedMessage) {
	fmt.Println("NACK request")
	queueName := msg.Head.Routing
	s.mu.RLock()
	q, exists := s.Queues[queueName]
	s.mu.RUnlock()
	if !exists {
		fmt.Printf("NACK error: queue %q not found\n", queueName)
		return
	}

	var msgID uuid.UUID
	if err := msgID.UnmarshalText(msg.PayLoad); err != nil {
		fmt.Printf("NACK error: bad message id: %v\n", err)
		return
	}
	if err := q.HandleNack(msgID); err != nil {
		fmt.Printf("NACK error: %v\n", err)
	}
}

// ---------------------------------------------------------------------------
// Helpers – send simple JSON ack/error back to the client
// ---------------------------------------------------------------------------

type serverResponse struct {
	Status  string `json:"status"`
	Method  string `json:"method"`
	Message string `json:"message"`
}

func sendOK(conn net.Conn, method, message string) {
	resp := serverResponse{Status: "ok", Method: method, Message: message}
	data, _ := json.Marshal(resp)
	data = append(data, '\n')
	conn.Write(data)
}

func sendError(conn net.Conn, method, message string) {
	resp := serverResponse{Status: "error", Method: method, Message: message}
	data, _ := json.Marshal(resp)
	data = append(data, '\n')
	conn.Write(data)
}
