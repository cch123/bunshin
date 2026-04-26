package bunshin

import (
	"context"
	"testing"
	"time"
)

func TestPublicationSubscriptionSoak(t *testing.T) {
	if testing.Short() {
		t.Skip("soak test is skipped in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	received := make(chan uint64, 1024)
	sub, err := ListenSubscription(SubscriptionConfig{
		StreamID:  130,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	go func() {
		_ = sub.Serve(ctx, func(_ context.Context, msg Message) error {
			received <- msg.Sequence
			return nil
		})
	}()

	pub, err := DialPublication(PublicationConfig{
		StreamID:   130,
		SessionID:  230,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	const messages = 1024
	for i := 0; i < messages; i++ {
		if err := pub.Send(ctx, []byte("soak")); err != nil {
			t.Fatal(err)
		}
	}
	for want := uint64(1); want <= messages; want++ {
		select {
		case got := <-received:
			if got != want {
				t.Fatalf("received sequence = %d, want %d", got, want)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}
