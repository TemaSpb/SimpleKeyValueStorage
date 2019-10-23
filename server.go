package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
)

const (
	PUT      = "PUT"
	READ     = "READ"
	DELETE   = "DELETE"
	ConnHost = "localhost"
	ConnPort = "3333"
)

type Command interface {
	run()
}

var storage map[string]string
var lock = sync.RWMutex{}

func main() {
	log.SetOutput(os.Stdout)
	ln, err := net.Listen("tcp", ":3333")
	if err != nil {
		log.Println("Error listening:", err)
		os.Exit(1)
	}

	defer ln.Close()
	log.Println("Listening on " + ConnHost + ":" + ConnPort)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("Error accepting: ", err)
			os.Exit(1)
		}
		go connectionHandler(conn)
	}
}

func connectionHandler(conn net.Conn) {
	for {
		// Receive a new command
		command, err := bufio.NewReader(conn).ReadString('\n')

		if err != nil {
			if err == io.EOF {
				log.Println("Error io.EOF", err)
				// Break the loop if no more input is available
				break
			} else {
				log.Println("Error reading:", err)
			}
		}

		if strings.HasPrefix(command, PUT) {
			putC := &PutCommand{
				Text: command,
				Conn: conn,
			}

			putC.run()
		} else if strings.HasPrefix(command, READ) {
			readC := &ReadCommand{
				Text: command,
				Conn: conn,
			}

			readC.run()
		} else if strings.HasPrefix(command, DELETE) {
			deleteC := &DeleteCommand{
				Text: command,
				Conn: conn,
			}

			deleteC.run()
		} else {
			log.Println("Wong command")
		}
	}
}

type PutCommand struct {
	Text       string
	Key        string
	Value      string
	ExpireTime string
	Conn       net.Conn
}

type ReadCommand struct {
	Text string
	Key  string
	Conn net.Conn
}

type DeleteCommand struct {
	Text string
	Key  string
	Conn net.Conn
}

// Read value by key
func (g *ReadCommand) run() {
	err := parseTextCommand(func() error {
		_, err := fmt.Sscanf(g.Text, READ+" %s\n", &g.Key)
		return err
	})

	if err == nil {
		value, ok := read(g.Key)
		if ok {
			_, err := g.Conn.Write([]byte(value))
			if err != nil {
				log.Println("Error occurred while sending the value", err)
			}
		} else {
			log.Printf("Key %s doesn't exist", g.Key)
		}
	}
}

// Put a new key value pair
func (g *PutCommand) run() {
	err := parseTextCommand(func() error {
		_, err := fmt.Sscanf(g.Text, PUT+" %s %s %s\n", &g.Key, &g.Value, &g.ExpireTime)
		return err
	})

	if err == nil {
		write(g.Key, g.Value)
		response := fmt.Sprintf("Key-value pair added %s:%s", g.Key, g.Value)
		_, err := g.Conn.Write([]byte(response))
		if err != nil {
			log.Println("Error occurred while sending the value", err)
		}
	}
}

// Remove value by key
func (g *DeleteCommand) run() {
	err := parseTextCommand(func() error {
		_, err := fmt.Sscanf(g.Text, DELETE+" %s\n", &g.Key)
		return err
	})

	if err == nil {
		remove(g.Key)
		response := fmt.Sprintf("Value with key %s removed", g.Key)
		_, err := g.Conn.Write([]byte(response))
		if err != nil {
			log.Println("Error occurred while sending the value", err)
		}
	}
}

func read(key string) (string, bool) {
	lock.RLock()
	defer lock.RUnlock()
	value, ok := storage[key]
	return value, ok
}

func write(key, value string) {
	lock.Lock()
	defer lock.Unlock()
	storage[key] = value
}

func remove(key string) {
	lock.Lock()
	defer lock.Unlock()
	delete(storage, key)
}

func parseTextCommand(parse func() error) error {
	err := parse()

	if err != nil {
		log.Println("Can't parse command", err)
		return err
	}
	return nil
}
