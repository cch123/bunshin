package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/xargin/bunshin"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:40456", "subscription address")
	payload := flag.String("payload", "hello", "payload to publish")
	flag.Parse()

	pub, err := bunshin.DialPublication(bunshin.PublicationConfig{
		StreamID:   1,
		RemoteAddr: *addr,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pub.Send(ctx, []byte(*payload)); err != nil {
		log.Fatal(err)
	}
}
