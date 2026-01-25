package main

import (
	"github.com/Max20050/go_message_broker/server"
)

type Request struct {
	Type string // Publish/Consume/ack
}

func main() {

	serv, err := server.CreteTcpServer("8080")
	if err != nil {
		panic(err.Error())
		return
	}

	serv.Accept() // Loop for accepting connections and start communication with clients

}
