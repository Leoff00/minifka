package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/google/uuid"
)

type Client struct {
	isProd bool
	topic  string
	conn   net.Conn
}

type Broker struct {
	mu        sync.Mutex
	consumers map[string][]*Client
}

func NewBroker() *Broker {
	return &Broker{
		consumers: make(map[string][]*Client),
	}
}

func (b *Broker) AddConsumer(topic string, c *Client) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consumers[topic] = append(b.consumers[topic], c)
	fmt.Printf("[Broker] Consumer successfully registered: '%s'\n", topic)
}

func (b *Broker) Broadcast(topic, msg string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, c := range b.consumers[topic] {
		fmt.Fprintf(c.conn, "%s\n", msg)
	}
	fmt.Printf("[Broker] Broadcast in '%s', message: %s\n", topic, msg)

	msgId, err := uuid.NewV7()
	if err != nil {
		fmt.Println("couldn't generate message id")
		return "", err
	}

	return msgId.String(), nil
}

func handleConn(b *Broker, conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)

	line, err := r.ReadString('\n')
	if err != nil {
		return
	}
	parts := strings.Fields(strings.TrimSpace(line))

	if len(parts) != 2 {
		return
	}

	role, topic := parts[0], parts[1]

	client := &Client{conn: conn, isProd: role == "PRODUCER", topic: topic}

	if !client.isProd {
		b.AddConsumer(topic, client)
		select {} //lock da app
	} else {
		for {
			msg, err := r.ReadString('\n')
			if err != nil {
				return
			}
			msg = strings.TrimSpace(msg)
			if msg == "" {
				continue
			}
			b.Broadcast(topic, msg)
		}
	}
}

func main() {
	broker := NewBroker()
	ln, err := net.Listen("tcp", ":9000")
	if err != nil {
		panic(err)
	}
	fmt.Println("[Broker] Listening on PORT 9000")
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go handleConn(broker, conn)
	}
}
