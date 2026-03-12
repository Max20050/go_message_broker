package main

import (
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
			// Return a shallow copy so the admin reads don't hold the lock.
			cp := make(map[string]*queues.Queue, len(serv.Queues))
			for k, v := range serv.Queues {
				cp[k] = v
			}
			return cp
		},
		Exchanges: serv.Exchanges,
	}
	go adminSrv.Start(nil)

	serv.Accept() // Loop for accepting connections
}
