package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	PUT      = "PUT"
	READ     = "READ"
	DELETE   = "DELETE"
	ConnHost = "localhost"
	ConnPort = "3333"
	FileName = "data.json"
)

type PutCommand struct {
	Text     string
	Key      string
	Value    string
	LifeTime int
	Conn     net.Conn
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

type Command interface {
	run() error
}

type ValueWithExpireDate struct {
	Value      string
	ExpireDate time.Time
}

var storage map[string]ValueWithExpireDate
var lock = sync.RWMutex{}

func main() {
	log.SetOutput(os.Stdout)
	ln, err := net.Listen("tcp", ":3333")
	if err != nil {
		log.Println("Error listening:", err)
		os.Exit(1)
	}

	ticker := time.NewTicker(10 * time.Second)

	if _, err := os.Stat(FileName); err == nil {
		storage = readJSON()
	} else {
		storage = make(map[string]ValueWithExpireDate)
	}

	go saveStorage(*ticker)

	defer ln.Close()
	defer writeJSON()
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

func writeJSON() {
	if len(storage) == 0 {
		return
	}
	jsonData, err := json.Marshal(storage)
	if err != nil {
		log.Println("Can't marshal storage", err)
	}
	err = ioutil.WriteFile(FileName, jsonData, 0644)
	if err != nil {
		log.Println("Can't save storage", err)
	}
}

func readJSON() map[string]ValueWithExpireDate {
	data := map[string]ValueWithExpireDate{}
	file, _ := ioutil.ReadFile(FileName)
	err := json.Unmarshal(file, &data)
	if err != nil {
		log.Println("Can't parse json with data")
		// If something goes wrong we just initialize a new empty map
		return make(map[string]ValueWithExpireDate)
	}
	return data
}

func saveStorage(ticker time.Ticker) {
	for {
		t := <-ticker.C
		writeJSON()
		log.Println("Saved current state of storage", t)
	}
}

func connectionHandler(conn net.Conn) {
	for {
		// Receive a new command
		command, err := bufio.NewReader(conn).ReadString('\n')

		if err != nil {
			if err != io.EOF {
				log.Println(err)
			}
			break
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
			respondToClient(&conn, "Wrong command")
		}
	}
	conn.Close()
}

// Read value by key
func (g *ReadCommand) run() {
	_, err := fmt.Sscanf(g.Text, READ+" %s\n", &g.Key)
	if err != nil {
		respondToClient(&g.Conn, "Can't parse command")
		return
	}

	value, ok := read(g.Key)
	if ok {
		// Return value if all ok
		respondToClient(&g.Conn, value)
		return
	}

	// Case when value with this key doesn't exist or key is expired
	respondToClient(&g.Conn, "Value with such key doesn't exist")
}

// Put a new key value pair
func (g *PutCommand) run() {
	_, err := fmt.Sscanf(g.Text, PUT+" %s %s %d\n", &g.Key, &g.Value, &g.LifeTime)
	if err != nil {
		respondToClient(&g.Conn, "Can't parse command")
		return
	}

	write(g.Key, g.Value, g.LifeTime)
	respondToClient(&g.Conn, fmt.Sprintf("Key-value pair added %s:%s", g.Key, g.Value))
}

// Remove value by key
func (g *DeleteCommand) run() {
	_, err := fmt.Sscanf(g.Text, DELETE+" %s\n", &g.Key)
	if err != nil {
		respondToClient(&g.Conn, "Can't parse command")
		return
	}

	remove(g.Key)
	respondToClient(&g.Conn, fmt.Sprintf("Value with key %s removed", g.Key))
}

func read(key string) (string, bool) {
	lock.RLock()
	defer lock.RUnlock()
	record, ok := storage[key]
	if ok {
		if record.ExpireDate.Before(time.Now()) {
			delete(storage, key)
			log.Printf("Key %s is expired", key)
			return "", false
		}
		return record.Value, ok
	}
	return "", ok
}

func write(key, value string, lifetime int) {
	lock.Lock()
	defer lock.Unlock()
	storage[key] = ValueWithExpireDate{
		Value:      value,
		ExpireDate: time.Now().Add(time.Second * time.Duration(lifetime)),
	}
}

func remove(key string) {
	lock.Lock()
	defer lock.Unlock()
	delete(storage, key)
}

func respondToClient(conn *net.Conn, response string) {
	_, err := (*conn).Write([]byte(response + "\n"))
	if err != nil {
		log.Println("Error occurred while sending the value", err)
	}
}
