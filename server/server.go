package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"

	"github.com/Max20050/go_message_broker/models"
	"github.com/Max20050/go_message_broker/queues"
	"github.com/google/uuid"
)

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

// We accept the tcp connections to our server and create a worker to handle the communication
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
		fmt.Println("message received")
		jsonData := scanner.Bytes()

		// DEBUG: Print the exact JSON received
		fmt.Printf("Raw JSON: %s\n", string(jsonData))

		var msg models.RecievedMessage
		if err := json.Unmarshal(jsonData, &msg); err != nil {
			fmt.Printf("Error unmarshalling JSON: %v\n", err)
			fmt.Printf("JSON was: %s\n", string(jsonData))
			continue
		}
		if msg.Head.Method == "PUBLISH" {
			q, exists := s.Queues[msg.Head.QueueName]
			if !exists {
				queue := queues.CreateQueue(msg.Head.QueueName, 1000) // Replace with DeclareQueue -> Needed for consumers and good practice
				s.Queues[msg.Head.QueueName] = &queue

				s.Queues[msg.Head.QueueName].Enqueue(msg.ToStorage())
			} else {
				q.Enqueue(msg.ToStorage())
			}
			fmt.Println("Message published")
		}
		if msg.Head.Method == "CONSUME" {
			fmt.Println("consume initiated")
			q, exists := s.Queues[msg.Head.QueueName]
			if !exists {
				// Return error
			} else {

				var ConsumerPayload models.ConsumerPayload

				if err := json.Unmarshal(msg.PayLoad, &ConsumerPayload); err != nil {
					panic(err.Error())
				}
				consumer := models.Consumer{
					QueueName:   msg.Head.QueueName,
					ConsumerTag: msg.Head.Issuer,
					AutoAck:     ConsumerPayload.AutoAck,
				}
				q.Consumers[consumer.ConsumerTag] = consumer

				go q.StartDispacher(conn, consumer.ConsumerTag)
			}
			fmt.Println("Message Consumed")
		}
		if msg.Head.Method == "ACK" {
			fmt.Println("ACK request")
			q, exists := s.Queues[msg.Head.QueueName]
			if !exists {
				// Return error
			}

			var msgID uuid.UUID
			err := msgID.UnmarshalText(msg.PayLoad)
			if err != nil {
				panic(err.Error())
			}
			fmt.Println(msgID)
			err = q.HandleAck(msgID)
			if err != nil {
				panic(err.Error())
			}
		}
		if msg.Head.Method == "NACK" {
			fmt.Println("ACK request")
			q, exists := s.Queues[msg.Head.QueueName]
			if !exists {
				// Return error
			}

			var msgID uuid.UUID
			err := msgID.UnmarshalText(msg.PayLoad)
			if err != nil {
				panic(err.Error())
			}
			fmt.Println(msgID)
			err = q.HandleNack(msgID)
			if err != nil {
				panic(err.Error())
			}
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading from connection: %v\n", err)
	}
}
