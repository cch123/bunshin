package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/xargin/bunshin"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:40456", "listen address")
	flag.Parse()

	sub, err := bunshin.ListenSubscription(bunshin.SubscriptionConfig{
		StreamID:  1,
		LocalAddr: *addr,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	fmt.Printf("listening on %s\n", sub.LocalAddr())
	if err := sub.Serve(context.Background(), func(_ context.Context, msg bunshin.Message) error {
		fmt.Printf("stream=%d session=%d sequence=%d payload=%s\n", msg.StreamID, msg.SessionID, msg.Sequence, msg.Payload)
		return nil
	}); err != nil {
		log.Fatal(err)
	}
}
