package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/xargin/bunshin"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server, err := bunshin.ListenSubscription(bunshin.SubscriptionConfig{
		StreamID:  1,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer server.Close()

	go func() {
		_ = server.Serve(ctx, func(ctx context.Context, msg bunshin.Message) error {
			replyAddr, body, ok := strings.Cut(string(msg.Payload), "\n")
			if !ok || !strings.HasPrefix(replyAddr, "reply=") {
				return fmt.Errorf("request is missing reply address")
			}
			pub, err := bunshin.DialPublication(bunshin.PublicationConfig{
				StreamID:   2,
				RemoteAddr: strings.TrimPrefix(replyAddr, "reply="),
			})
			if err != nil {
				return err
			}
			defer pub.Close()
			return pub.Send(ctx, []byte("response: "+body))
		})
	}()

	replies, err := bunshin.ListenSubscription(bunshin.SubscriptionConfig{
		StreamID:  2,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer replies.Close()

	responseCh := make(chan bunshin.Message, 1)
	go func() {
		_ = replies.Serve(ctx, func(_ context.Context, msg bunshin.Message) error {
			responseCh <- msg
			return nil
		})
	}()

	client, err := bunshin.DialPublication(bunshin.PublicationConfig{
		StreamID:   1,
		RemoteAddr: server.LocalAddr().String(),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	request := "reply=" + replies.LocalAddr().String() + "\nhello"
	if err := client.Send(ctx, []byte(request)); err != nil {
		log.Fatal(err)
	}

	select {
	case response := <-responseCh:
		fmt.Println(string(response.Payload))
	case <-ctx.Done():
		log.Fatal(ctx.Err())
	}
}
