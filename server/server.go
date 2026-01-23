package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
)

type Headers struct {
	Method string `json:"method"` // Publish/Consume
	Issuer string `json:"issuer"` //e.g: Backend
}

type Message struct {
	Head    Headers `json:"headers"`
	PayLoad string  `json:"payload"`
}

type Server struct {
	Port     string // Running server port
	Listener net.Listener
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
	}, nil
}

func (s *Server) Accept() error {
	for {
		conn, err := s.Listener.Accept()
		if err != nil {
			log.Println("Error accepting connection:", err)
			continue
		}

		// 3. Handle each connection concurrently in a new goroutine
		go handleConnection(conn)
	}

}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		jsonData := scanner.Bytes()
		var msg Message
		if err := json.Unmarshal(jsonData, &msg); err != nil {
			fmt.Printf("Error unmarshalling JSON: %v\n", err)
			continue
		}
		fmt.Println(msg)
		// ... (optional reply logic)
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading from connection: %v\n", err)
	}
}
