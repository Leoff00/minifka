// producer.go
package main

import (
	"fmt"
	"net"
	"time"
)

func main() {
	topic := "foo_topic"
	conn, err := net.Dial("tcp", "localhost:9000")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	fmt.Fprintf(conn, "PRODUCER %s\n", topic)

	for i := 1; i <= 10; i++ {
		msg := fmt.Sprintf("Event #%d at %s", i, time.Now().Format(time.RFC3339))
		fmt.Fprintf(conn, "%s\n", msg)
		fmt.Println("[Producer] Published:", msg)
	}

}
