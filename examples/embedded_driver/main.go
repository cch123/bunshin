package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/xargin/bunshin"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	driver, err := bunshin.StartMediaDriver(bunshin.DriverConfig{})
	if err != nil {
		log.Fatal(err)
	}
	defer driver.Close()

	client, err := driver.NewClient(ctx, "example")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close(context.Background())

	sub, err := client.AddSubscription(ctx, bunshin.SubscriptionConfig{
		StreamID:  1,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		log.Fatal(err)
	}

	received := make(chan bunshin.Message, 1)
	go func() {
		_ = sub.Serve(ctx, func(_ context.Context, msg bunshin.Message) error {
			received <- msg
			return nil
		})
	}()

	pub, err := client.AddPublication(ctx, bunshin.PublicationConfig{
		StreamID:   1,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := pub.Send(ctx, []byte("driver message")); err != nil {
		log.Fatal(err)
	}

	select {
	case msg := <-received:
		fmt.Println(string(msg.Payload))
	case <-ctx.Done():
		log.Fatal(ctx.Err())
	}
}
