package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"

	"github.com/Max20050/go_message_broker/queues"
)

type Exchange struct {
}

type Server struct {
	Port     string // Running server port
	Listener net.Listener
	Queues   map[string]*queues.Queue
}

func CreteTcpServer(port string) (Server, error) {
	Listener, err := net.Listen("tcp", "localhost:"+port)
	if err != nil {
		return Server{}, err
	}

	fmt.Println("Server Listening in: ", port)

	return Server{
		Port:     port,
		Listener: Listener,
		Queues:   make(map[string]*queues.Queue),
	}, nil
}

func (s *Server) Accept() error {
	for {
		conn, err := s.Listener.Accept()
		if err != nil {
			log.Println("Error accepting connection:", err)
			continue
		}

		go s.handleConnection(conn) // Worker per connection
	}

}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	fmt.Println(conn.RemoteAddr().String())
	for scanner.Scan() {
		fmt.Println("message recieved")
		jsonData := scanner.Bytes()
		var msg queues.Message
		if err := json.Unmarshal(jsonData, &msg); err != nil {
			fmt.Printf("Error unmarshalling JSON: %v\n", err)
			continue
		}
		if msg.Head.Method == "PUBLISH" {
			q, exists := s.Queues[msg.Head.QueueName]
			if !exists {
				queue := queues.CreateQueue(msg.Head.QueueName, 1000)
				s.Queues[msg.Head.QueueName] = &queue

				s.Queues[msg.Head.QueueName].Enqueue(msg)
			} else {

				q.Enqueue(msg)
			}
			fmt.Println("Message published")
		}
		if msg.Head.Method == "CONSUME" {
			fmt.Println("consume initiated")
			q, exists := s.Queues[msg.Head.QueueName]
			if !exists {
				// Return error
			} else {
				q.StartDispacher(conn)
			}
			fmt.Println("Message Consumed")
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading from connection: %v\n", err)
	}
}
