// consumer.go
package main

import (
	"bufio"
	"fmt"
	"net"
)

func main() {
	topic := "foo_topic"
	conn, err := net.Dial("tcp", "localhost:9000")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	fmt.Fprintf(conn, "CONSUMER %s\n", topic)
	reader := bufio.NewReader(conn)

	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("[Consumer] Conexão terminated.")
			return
		}
		fmt.Print("[Consumer] Received: ", msg)
	}
}
