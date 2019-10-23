package main

import (
	"fmt"
	"log"
	"net"
)

func main() {
	fmt.Println("start client")
	conn, err := net.Dial("tcp", "localhost:3333")
	if err != nil {
		log.Fatal("Connection error", err)
	}

	conn.Write([]byte("READ asd\n\r"))
	conn.Close()
	fmt.Println("done")
}
