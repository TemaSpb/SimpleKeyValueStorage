package main

import (
	"bufio"
	"container/heap"
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
var pq PriorityQueue

func main() {
	log.SetOutput(os.Stdout)
	ln, err := net.Listen("tcp", ":3333")
	if err != nil {
		log.Println("Error listening:", err)
		os.Exit(1)
	}

	if _, err := os.Stat(FileName); err == nil {
		storage = readJSON()
	} else {
		storage = make(map[string]ValueWithExpireDate)
	}

	initializePriorityQueue()

	tickerForSaving := time.NewTicker(30 * time.Second)
	tickerForKeyExpiration := time.NewTicker(10 * time.Second)

	go saveStorage(*tickerForSaving)
	go removeExpiredKeysScheduler(*tickerForKeyExpiration)

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

func removeExpiredKeys() {
	if pq.Len() > 0 {
		log.Println(pq.getMin().expireDate)
		log.Println(time.Now())
	}
	for pq.Len() > 0 && pq.getMin().expireDate.Before(time.Now()) {
		item := heap.Pop(&pq).(*Item)
		log.Printf("Key %s is expired", item.value)
	}
}

func initializePriorityQueue() {
	pq := make(PriorityQueue, len(storage))
	i := 0
	for key, value := range storage {
		pq[i] = &Item{
			value:      key,
			expireDate: value.ExpireDate,
			index:      i,
		}
		i++
	}
	heap.Init(&pq)
}

func writeJSON() bool {
	if len(storage) == 0 {
		return false
	}
	jsonData, err := json.Marshal(storage)
	if err != nil {
		log.Println("Can't marshal storage", err)
		return false
	}
	err = ioutil.WriteFile(FileName, jsonData, 0644)
	if err != nil {
		log.Println("Can't save storage", err)
		return false
	}
	return true
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

func removeExpiredKeysScheduler(ticker time.Ticker) {
	for {
		_ = <-ticker.C
		removeExpiredKeys()
	}
}

func saveStorage(ticker time.Ticker) {
	for {
		_ = <-ticker.C
		ok := writeJSON()
		if ok {
			//log.Println("Saved current state of storage", t)
		}
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
	expireDate := time.Now().Add(time.Second * time.Duration(lifetime))
	log.Println("Expire date ", expireDate)
	storage[key] = ValueWithExpireDate{
		Value:      value,
		ExpireDate: expireDate,
	}
	item := &Item{
		value:      key,
		expireDate: expireDate,
	}
	heap.Push(&pq, item)
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
