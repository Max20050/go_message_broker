# GoMQ – Lightweight Message Broker

A lightweight, AMQP-inspired message broker written in Go. GoMQ provides queue-based messaging over TCP with support for **direct**, **fanout**, and **topic** exchange types, manual/auto acknowledgement, and a built-in web admin panel.

---

## Table of Contents

- [Architecture](#architecture)
- [Getting Started](#getting-started)
- [Client Library](#client-library)
  - [Connecting](#connecting)
  - [Channels](#channels)
  - [Declaring Queues](#declaring-queues)
  - [Declaring Exchanges](#declaring-exchanges)
  - [Binding Queues](#binding-queues)
  - [Publishing Messages](#publishing-messages)
  - [Consuming Messages](#consuming-messages)
  - [Acknowledgements (ACK / NACK)](#acknowledgements-ack--nack)
- [Exchange Types](#exchange-types)
  - [Direct Exchange](#direct-exchange)
  - [Fanout Exchange](#fanout-exchange)
  - [Topic Exchange](#topic-exchange)
- [Admin Panel](#admin-panel)
  - [Login](#login)
  - [Dashboard](#dashboard)
  - [Viewing Messages](#viewing-messages)
  - [Manual ACK from Admin](#manual-ack-from-admin)
  - [REST API Endpoints](#rest-api-endpoints)
- [Wire Protocol](#wire-protocol)
- [Project Structure](#project-structure)
- [Running the Examples](#running-the-examples)

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                       GoMQ Broker                            │
│                                                              │
│   TCP :8080                          HTTP :15672             │
│   ┌──────────────┐                   ┌──────────────────┐    │
│   │  TCP Server   │                  │   Admin Panel     │    │
│   │  (auth, mux)  │                  │  (login, API,     │    │
│   └──────┬───────┘                   │   dashboard)      │    │
│          │                           └────────┬─────────┘    │
│          ▼                                    │              │
│   ┌──────────────┐    ┌──────────────┐        │              │
│   │  Exchanges    │───▶│   Queues      │◀──────┘              │
│   │  (registry)   │    │  (channels,   │                      │
│   │               │    │   overflow,    │                      │
│   │  direct       │    │   in-flight)   │                      │
│   │  fanout       │    └──────┬───────┘                      │
│   │  topic        │           │                               │
│   └──────────────┘           ▼                               │
│                       ┌──────────────┐                       │
│                       │  Consumers    │                       │
│                       │  (dispatcher) │                       │
│                       └──────────────┘                       │
└──────────────────────────────────────────────────────────────┘
```

**Key components:**

| Component      | Description |
|---------------|-------------|
| **TCP Server** | Accepts client connections on port `8080`, handles authentication, and dispatches protocol messages. |
| **Exchanges**  | Route messages to queues based on type and routing key. Three types: `direct`, `fanout`, `topic`. |
| **Queues**     | Buffered channel + overflow list. Each queue tracks in-flight messages and registered consumers. |
| **Dispatcher** | Per-consumer goroutine that delivers messages from a queue to the consumer's TCP connection. |
| **Admin Panel**| HTTP server on port `15672` with a web dashboard and REST API for monitoring and management. |
| **Client Lib** | Go package (`client`) to connect, publish, consume, and acknowledge messages. |

---

## Getting Started

### Prerequisites

- **Go 1.21+**

### Start the Broker

```bash
cd cmd/app
go run main.go
```

This starts:
- **TCP broker** on `localhost:8080`
- **Admin panel** on `http://localhost:15672`

### Default Credentials

Both the broker and the admin panel use hardcoded credentials:

| Username | Password |
|----------|----------|
| `root`   | `root`   |

---

## Client Library

Import the client package:

```go
import "github.com/Max20050/go_message_broker/client"
```

### Connecting

```go
broker, err := client.ConnectBroker("localhost", "8080", "root", "root")
if err != nil {
    panic(err)
}
```

The `ConnectBroker` function establishes a TCP connection and performs the authentication handshake. If credentials are invalid, it returns an error.

### Channels

Channels are logical multiplexed paths over a single TCP connection. All operations (declare, publish, consume) are scoped to a channel:

```go
ch := broker.OpenChannel()
```

You can open multiple channels on the same broker connection. Each channel gets an auto-incrementing ID.

### Declaring Queues

Before publishing or consuming, declare the queue:

```go
err := ch.DeclareQueue("emails", 1000)
```

| Parameter | Description |
|-----------|-------------|
| `name`    | Queue name (unique identifier) |
| `size`    | Buffer capacity. Messages beyond this go to an overflow list. Set to `0` for default (1000). |

Declaring an already-existing queue is a no-op (idempotent).

### Declaring Exchanges

```go
err := ch.DeclareExchange("logs", "fanout")
```

| Parameter | Description |
|-----------|-------------|
| `name`    | Exchange name |
| `kind`    | Exchange type: `"direct"`, `"fanout"`, or `"topic"` |

A **default direct exchange** (name = `""`) is always available. When you publish to the default exchange, the routing key is treated as the queue name.

### Binding Queues

Bind a queue to a named exchange:

```go
err := ch.BindQueue("log_console", "logs", "")
```

| Parameter      | Description |
|---------------|-------------|
| `queueName`    | Queue to bind |
| `exchangeName` | Target exchange |
| `routingKey`   | For direct/topic exchanges. Ignored by fanout. |

### Publishing Messages

```go
message := map[string]interface{}{
    "from":    "user@example.com",
    "subject": "Hello",
    "content": "World",
}

// Point-to-point (default exchange, routing key = queue name)
err := ch.Publish(ctx, "", "emails", "MyService", message)

// Fanout (all bound queues receive the message)
err := ch.Publish(ctx, "logs", "", "Logger", message)

// Topic (pattern-matched routing)
err := ch.Publish(ctx, "events", "payments.due", "PaymentService", message)
```

| Parameter      | Description |
|---------------|-------------|
| `exchangeName` | Exchange to publish to (`""` = default direct) |
| `routingKey`   | Routing key for direct/topic routing |
| `issuer`       | Name identifying the publisher |
| `message`      | Any JSON-serializable value |

### Consuming Messages

```go
msgs, err := ch.Consume("emails", "EmailWorker", false)
if err != nil {
    panic(err)
}

for msg := range msgs {
    fmt.Println("Received:", string(msg.PayLoad))
    msg.Ack() // or msg.Nack() to requeue
}
```

| Parameter      | Description |
|---------------|-------------|
| `queueName`    | Queue to consume from |
| `consumerTag`  | Unique name for this consumer |
| `autoAck`      | If `true`, messages are acknowledged automatically. If `false`, you must call `msg.Ack()`. |

The returned channel (`<-chan MessageConsumer`) delivers messages as they arrive. It closes when the connection drops.

### Acknowledgements (ACK / NACK)

When `autoAck` is `false`, messages go into the **in-flight** state after delivery. They remain in-flight until explicitly acknowledged:

```go
msg.Ack()   // ✅ Message is confirmed and removed from the queue
msg.Nack()  // ❌ Message is requeued for redelivery
```

**If a consumer disconnects** without ACKing, the dispatcher requeues unacknowledged messages automatically.

---

## Exchange Types

### Direct Exchange

Routes messages to queues whose **binding key exactly matches** the routing key.

```
Publisher ──("payments.due")──▶ Direct Exchange ──▶ Queue "billing" (bound with key "payments.due")
```

The **default exchange** (`""`) is a special direct exchange that auto-binds every queue using the queue name as the routing key.

### Fanout Exchange

Delivers every message to **ALL bound queues**, regardless of the routing key.

```
Publisher ──("anything")──▶ Fanout Exchange ──▶ Queue "log_console"
                                             ──▶ Queue "log_file"
                                             ──▶ Queue "log_db"
```

### Topic Exchange

Matches the routing key against **AMQP-style patterns**:

| Pattern | Matches |
|---------|---------|
| `*`     | Exactly one word |
| `#`     | Zero or more words |

Words are separated by `.` (dots).

```
Routing Key: "payments.due"

Pattern "payments.due"   → ✅ exact match
Pattern "payments.*"     → ✅ matches any single word after "payments."
Pattern "payments.#"     → ✅ matches zero or more words after "payments."
Pattern "#"              → ✅ matches everything
Pattern "orders.*"       → ❌ no match
```

---

## Admin Panel

The admin panel runs on **port 15672** (same as RabbitMQ by convention) and provides a web-based dashboard for monitoring and managing the broker.

### Login

Navigate to `http://localhost:15672`. You'll be presented with a login screen.

| Field    | Value  |
|----------|--------|
| Username | `root` |
| Password | `root` |

Sessions last 24 hours and are stored in memory. Use the **Logout** button in the navbar to sign out.

### Dashboard

After login, the dashboard shows:

- **Overview cards** – Total queues, exchanges, consumers, and messages at a glance.
- **Queues tab** – Lists all declared queues with message count, capacity, usage bar, in-flight count, and consumer count.
- **Exchanges tab** – Lists all declared exchanges with type and bindings.
- **Consumers tab** – Lists all active consumers with their tag, queue, channel, and auto-ack status.

The dashboard **auto-refreshes every 3 seconds**. You can also click the **↻ Refresh** button to refresh manually.

### Viewing Messages

Click on any **queue name** or the **View** button in the Queues tab to open the Messages modal. This shows:

| Column     | Description |
|------------|-------------|
| **Status** | `queued` (in buffer) or `inflight` (delivered, awaiting ACK) |
| **Message ID** | UUID assigned by the broker |
| **Issuer**     | Publisher identifier |
| **Routing**    | Routing key used to publish |
| **Timestamp**  | When the message was published |
| **Payload**    | Message body (click to expand as formatted JSON) |
| **Action**     | ACK button to remove the message |

### Manual ACK from Admin

You can **ACK (remove) any message** from the admin panel, regardless of its state:

- **In-flight messages** – Removed from the in-flight map (as if the consumer acknowledged them).
- **Queued messages** – Removed from the queue buffer (the message is dropped and will not be delivered).

This is useful for:
- Removing poison messages that are causing consumer failures
- Clearing stuck in-flight messages from dead consumers
- Manually draining a queue

### REST API Endpoints

All API endpoints require authentication (session cookie). Returns JSON.

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST`  | `/api/login`     | Authenticate. Body: `{"username":"…","password":"…"}` |
| `POST`  | `/api/logout`    | Invalidate session |
| `GET`   | `/api/overview`  | Summary stats (queue/exchange/consumer/message counts) |
| `GET`   | `/api/queues`    | List all queues with details |
| `GET`   | `/api/exchanges` | List all exchanges with bindings |
| `GET`   | `/api/consumers` | List all active consumers |
| `GET`   | `/api/messages?queue=NAME` | List all messages in a specific queue |
| `POST`  | `/api/ack`       | ACK a message. Body: `{"queue_name":"…","message_id":"…"}` |

**Example: Get all queues**
```bash
curl -b cookies.txt http://localhost:15672/api/queues
```

**Example: ACK a message**
```bash
curl -b cookies.txt -X POST http://localhost:15672/api/ack \
  -H "Content-Type: application/json" \
  -d '{"queue_name":"emails","message_id":"550e8400-e29b-41d4-a716-446655440000"}'
```

---

## Wire Protocol

GoMQ uses a simple **newline-delimited JSON** protocol over TCP. Each frame is a JSON object followed by `\n`.

### Client → Broker

```json
{
  "headers": {
    "method": "PUBLISH",
    "issuer": "MyService",
    "exchange": "",
    "routing": "emails",
    "channel_id": 1,
    "timestamp": "2026-03-13T22:00:00Z"
  },
  "payload": { "from": "user@example.com", "subject": "Hello" }
}
```

**Methods:** `AUTH`, `DECLARE_QUEUE`, `DECLARE_EXCHANGE`, `BIND_QUEUE`, `PUBLISH`, `CONSUME`, `ACK`, `NACK`

### Broker → Client (control-plane response)

```json
{
  "status": "ok",
  "method": "DECLARE_QUEUE",
  "message": "queue \"emails\" ready"
}
```

### Broker → Consumer (message delivery)

```json
{
  "headers": {
    "message_id": "550e8400-e29b-41d4-a716-446655440000",
    "method": "PUBLISH",
    "issuer": "MyService",
    "exchange": "",
    "routing": "emails",
    "channel_id": 1,
    "queuename": "emails",
    "timestamp": "2026-03-13T22:00:00Z"
  },
  "payload": { "from": "user@example.com", "subject": "Hello" }
}
```

---

## Project Structure

```
go_message_broker/
├── cmd/app/
│   └── main.go              # Entry point – starts TCP + Admin servers
├── server/
│   └── server.go            # TCP server, connection handler, protocol dispatch
├── Exchange/
│   └── exchange.go          # Exchange interface, Registry, Direct/Fanout/Topic implementations
├── queues/
│   └── queues.go            # Queue struct, enqueue/dequeue, dispatcher, ACK/NACK, peek
├── models/
│   ├── Messages.go          # Wire-level message types and payloads
│   └── Consumer.go          # Consumer struct
├── channel/
│   └── channel.go           # Server-side logical channel management
├── admin/
│   ├── admin.go             # HTTP server, auth middleware, API handlers
│   ├── dashboard.go         # Dashboard HTML (embedded Go string)
│   └── login.go             # Login page HTML (embedded Go string)
├── client/
│   └── client.go            # Client library (connect, publish, consume, ack/nack)
├── examples/
│   ├── publish.go           # Point-to-point publish example
│   ├── consume_Example/
│   │   └── consume.go       # Point-to-point consume example
│   ├── fanout_example/
│   │   └── fanout.go        # Fanout exchange example
│   └── topic_example/
│       └── topic.go         # Topic exchange example
├── go.mod
├── go.sum
└── TODO.md
```

---

## Running the Examples

> **Note:** Always start the broker first before running any example.

### 1. Start the Broker

```bash
go run cmd/app/main.go
```

### 2. Point-to-Point (Direct)

**Terminal 1 – Publisher:**
```bash
go run examples/publish.go
```

**Terminal 2 – Consumer:**
```bash
go run examples/consume_Example/consume.go
```

### 3. Fanout Exchange

```bash
go run examples/fanout_example/fanout.go
```

This declares a `logs` fanout exchange and two queues (`log_console`, `log_file`), then publishes messages that go to both.

### 4. Topic Exchange

```bash
go run examples/topic_example/topic.go
```

This declares an `events` topic exchange and two queues (`billing`, `audit`) with wildcard pattern bindings, then publishes different events to demonstrate pattern matching.

### 5. Admin Panel

Open `http://localhost:15672` in your browser and login with `root` / `root`.
