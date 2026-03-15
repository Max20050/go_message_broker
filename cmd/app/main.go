package main

import (
	"fmt"

	"github.com/Max20050/go_message_broker/admin"
	"github.com/Max20050/go_message_broker/queues"
	"github.com/Max20050/go_message_broker/server"
)

type Request struct {
	Type string // Publish/Consume/ack
}

func main() {
	serv, err := server.CreteTcpServer("8080")
	if err != nil {
		panic(err.Error())
	}

	// Start the admin HTTP panel on port 15672 (like RabbitMQ).
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
				return nil // idempotent
			}
			q := queues.CreateQueue(name, size)
			serv.Queues[name] = &q
			// NOTE: No auto-bind to default exchange here.
			// Admin-created queues only get the bindings explicitly
			// created through the editor/API. The TCP DECLARE_QUEUE
			// handler still auto-binds for client-created queues.
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

	serv.Accept() // Loop for accepting connections
}
