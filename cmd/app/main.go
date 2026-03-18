package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Max20050/go_message_broker/admin"
	"github.com/Max20050/go_message_broker/models"
	"github.com/Max20050/go_message_broker/persistence"
	"github.com/Max20050/go_message_broker/queues"
	"github.com/Max20050/go_message_broker/server"
	"github.com/google/uuid"
)

func main() {
	// ── Flags ────────────────────────────────────────────────────
	persist := flag.Bool("persist", true, "Enable state persistence to disk")
	dataDir := flag.String("data-dir", "data", "Directory for persistence files")
	saveInterval := flag.Int("save-interval", 5, "Seconds between periodic state saves")
	flag.Parse()

	// ── Create server ───────────────────────────────────────────
	serv, err := server.CreteTcpServer("8080")
	if err != nil {
		panic(err.Error())
	}

	// ── Persistence: restore ────────────────────────────────────
	var store *persistence.FileStore
	if *persist {
		store, err = persistence.NewFileStore(*dataDir)
		if err != nil {
			log.Fatalf("Failed to init persistence: %v", err)
		}
		fmt.Printf("Persistence enabled (dir: %s, interval: %ds)\n", *dataDir, *saveInterval)

		snap, err := store.Load()
		if err != nil {
			log.Fatalf("Failed to load state: %v", err)
		}
		restoreSnapshot(&serv, snap)
	} else {
		fmt.Println("Persistence disabled (use -persist to enable)")
	}

	// ── Admin panel ─────────────────────────────────────────────
	adminSrv := &admin.Server{
		Port: "15672",
		Queues: func() map[string]*queues.Queue {
			serv.GetMu().RLock()
			defer serv.GetMu().RUnlock()
			cp := make(map[string]*queues.Queue, len(serv.Queues))
			for k, v := range serv.Queues {
				cp[k] = v
			}
			return cp
		},
		DeclareQueue: func(name string, size int) error {
			serv.GetMu().Lock()
			defer serv.GetMu().Unlock()
			if _, exists := serv.Queues[name]; exists {
				return nil
			}
			q := queues.CreateQueue(name, size)
			serv.Queues[name] = &q
			fmt.Printf("Queue declared (admin): %s (size=%d)\n", name, size)
			return nil
		},
		BindQueue: func(queueName, exchangeName, routingKey string) error {
			serv.GetMu().RLock()
			q, qExists := serv.Queues[queueName]
			serv.GetMu().RUnlock()
			if !qExists {
				return fmt.Errorf("queue %q does not exist", queueName)
			}
			ex, exExists := serv.Exchanges.Get(exchangeName)
			if !exExists {
				return fmt.Errorf("exchange %q does not exist", exchangeName)
			}
			ex.Bind(routingKey, q)
			fmt.Printf("Queue %q bound to exchange %q with key %q (admin)\n", queueName, exchangeName, routingKey)
			return nil
		},
		Exchanges: serv.Exchanges,
	}
	go adminSrv.Start(nil)

	// ── Persistence: periodic save + graceful shutdown ──────────
	if *persist && store != nil {
		// Periodic save goroutine
		go func() {
			ticker := time.NewTicker(time.Duration(*saveInterval) * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				snap := takeSnapshot(&serv)
				if err := store.Save(snap); err != nil {
					log.Printf("[persistence] Save error: %v", err)
				}
			}
		}()

		// Graceful shutdown on SIGINT / SIGTERM
		go func() {
			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
			<-sigCh

			fmt.Println("\nShutting down — saving state...")
			snap := takeSnapshot(&serv)
			if err := store.Save(snap); err != nil {
				log.Printf("[persistence] Final save error: %v", err)
			} else {
				fmt.Println("State saved to", store.FilePath())
			}
			os.Exit(0)
		}()
	}

	serv.Accept()
}

// ── Snapshot: capture current broker state ───────────────────────────

func takeSnapshot(serv *server.Server) persistence.BrokerSnapshot {
	serv.GetMu().RLock()
	defer serv.GetMu().RUnlock()

	snap := persistence.BrokerSnapshot{}

	// Snapshot exchanges
	for _, exInfo := range serv.Exchanges.ListAll() {
		exSnap := persistence.ExchangeSnapshot{
			Name: exInfo.Name,
			Type: exInfo.Type,
		}
		for _, b := range exInfo.Bindings {
			exSnap.Bindings = append(exSnap.Bindings, persistence.BindingSnapshot{
				RoutingKey: b.RoutingKey,
				QueueName:  b.QueueName,
			})
		}
		snap.Exchanges = append(snap.Exchanges, exSnap)
	}

	// Snapshot queues + messages
	for name, q := range serv.Queues {
		buffered, inflight := q.PeekMessages()
		qSnap := persistence.QueueSnapshot{
			Name: name,
			Size: q.Capacity(),
		}
		// Buffered messages
		for _, m := range buffered {
			qSnap.Messages = append(qSnap.Messages, messageToSnapshot(m))
		}
		// In-flight messages (requeue on restore since consumers are gone)
		for _, m := range inflight {
			qSnap.Messages = append(qSnap.Messages, messageToSnapshot(m))
		}
		snap.Queues = append(snap.Queues, qSnap)
	}

	return snap
}

func messageToSnapshot(m models.StoredMessage) persistence.MessageSnapshot {
	return persistence.MessageSnapshot{
		MessageId: m.Head.MessageId.String(),
		Method:    m.Head.Method,
		Issuer:    m.Head.Issuer,
		Exchange:  m.Head.Exchange,
		Routing:   m.Head.Routing,
		ChannelID: m.Head.ChannelID,
		QueueName: m.Head.QueueName,
		Timestamp: m.Head.Timestamp,
		Payload:   m.PayLoad,
	}
}

// ── Restore: rebuild broker state from snapshot ─────────────────────

func restoreSnapshot(serv *server.Server, snap persistence.BrokerSnapshot) {
	if len(snap.Exchanges) == 0 && len(snap.Queues) == 0 {
		fmt.Println("No persisted state found, starting fresh.")
		return
	}

	fmt.Printf("Restoring state (saved at %s)...\n", snap.Timestamp.Format(time.RFC3339))

	// 1. Restore queues (must exist before bindings)
	for _, qs := range snap.Queues {
		q := queues.CreateQueue(qs.Name, qs.Size)
		serv.Queues[qs.Name] = &q

		// Restore messages
		for _, ms := range qs.Messages {
			msgId, err := uuid.Parse(ms.MessageId)
			if err != nil {
				log.Printf("[persistence] bad message ID %q, generating new one", ms.MessageId)
				msgId = uuid.New()
			}
			msg := models.StoredMessage{
				Head: models.Headers{
					MessageId: msgId,
					Method:    ms.Method,
					Issuer:    ms.Issuer,
					Exchange:  ms.Exchange,
					Routing:   ms.Routing,
					ChannelID: ms.ChannelID,
					QueueName: ms.QueueName,
					Timestamp: ms.Timestamp,
				},
				PayLoad: ms.Payload,
			}
			serv.Queues[qs.Name].EnqueueDirect(msg)
		}
		fmt.Printf("  ✓ Queue %q restored (%d messages)\n", qs.Name, len(qs.Messages))
	}

	// 2. Restore exchanges and bindings
	for _, es := range snap.Exchanges {
		// Skip the default exchange (it's auto-created by the registry)
		if es.Name == "" {
			// But still restore its non-default bindings
			defEx, _ := serv.Exchanges.Get("")
			for _, b := range es.Bindings {
				if q, ok := serv.Queues[b.QueueName]; ok {
					defEx.Bind(b.RoutingKey, q)
				}
			}
			continue
		}

		ex, err := serv.Exchanges.Declare(es.Name, es.Type)
		if err != nil {
			log.Printf("[persistence] Failed to restore exchange %q: %v", es.Name, err)
			continue
		}

		// Restore bindings
		for _, b := range es.Bindings {
			q, exists := serv.Queues[b.QueueName]
			if !exists {
				log.Printf("[persistence] Binding skipped: queue %q not found for exchange %q", b.QueueName, es.Name)
				continue
			}
			ex.Bind(b.RoutingKey, q)
		}
		fmt.Printf("  ✓ Exchange %q (%s) restored with %d binding(s)\n", es.Name, es.Type, len(es.Bindings))
	}

	// 3. Bind all queues to default exchange (for point-to-point)
	defaultEx, _ := serv.Exchanges.Get("")
	for name, q := range serv.Queues {
		defaultEx.Bind(name, q)
	}

	fmt.Println("State restored successfully.")
}
