package bunshin

import (
	"context"
	"testing"
	"time"
)

func TestPublicationSubscription(t *testing.T) {
	sub, err := ListenSubscription(SubscriptionConfig{
		StreamID:  99,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	received := make(chan Message, 1)
	go func() {
		_ = sub.Serve(ctx, func(_ context.Context, msg Message) error {
			received <- msg
			return nil
		})
	}()

	pub, err := DialPublication(PublicationConfig{
		StreamID:        99,
		SessionID:       1234,
		RemoteAddr:      sub.LocalAddr().String(),
		RetransmitEvery: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	if err := pub.Send(ctx, []byte("payload")); err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-received:
		if msg.StreamID != 99 || msg.SessionID != 1234 || msg.Sequence != 1 || string(msg.Payload) != "payload" {
			t.Fatalf("unexpected message: %#v", msg)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
