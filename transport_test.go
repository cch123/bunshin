package bunshin

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPublicationSubscription(t *testing.T) {
	pubMetrics := &Metrics{}
	subMetrics := &Metrics{}
	sub, err := ListenSubscription(SubscriptionConfig{
		StreamID:  99,
		LocalAddr: "127.0.0.1:0",
		Metrics:   subMetrics,
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
		InitialTermID:   77,
		RemoteAddr:      sub.LocalAddr().String(),
		RetransmitEvery: time.Millisecond,
		Metrics:         pubMetrics,
		ReservedValue: func(payload []byte) uint64 {
			return uint64(len(payload))
		},
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
		if msg.StreamID != 99 || msg.SessionID != 1234 || msg.TermID != 77 || msg.TermOffset != 0 ||
			msg.Sequence != 1 || string(msg.Payload) != "payload" {
			t.Fatalf("unexpected message: %#v", msg)
		}
		if msg.ReservedValue != uint64(len("payload")) {
			t.Fatalf("reserved value = %d, want %d", msg.ReservedValue, len("payload"))
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	pubSnapshot := pubMetrics.Snapshot()
	if pubSnapshot.ConnectionsOpened != 1 || pubSnapshot.MessagesSent != 1 || pubSnapshot.BytesSent != uint64(len("payload")) || pubSnapshot.AcksReceived != 1 {
		t.Fatalf("unexpected publication metrics: %#v", pubSnapshot)
	}
	subSnapshot := subMetrics.Snapshot()
	if subSnapshot.ConnectionsAccepted != 1 || subSnapshot.MessagesReceived != 1 || subSnapshot.BytesReceived != uint64(len("payload")) || subSnapshot.AcksSent != 1 {
		t.Fatalf("unexpected subscription metrics: %#v", subSnapshot)
	}
}

func TestPublicationBackPressureWaitsForAckCapacity(t *testing.T) {
	pubMetrics := &Metrics{}
	sub, err := ListenSubscription(SubscriptionConfig{
		StreamID:  100,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	received := make(chan Message, 1)
	releaseHandler := make(chan struct{})
	go func() {
		_ = sub.Serve(ctx, func(ctx context.Context, msg Message) error {
			received <- msg
			select {
			case <-releaseHandler:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})
	}()

	pub, err := DialPublication(PublicationConfig{
		StreamID:               100,
		RemoteAddr:             sub.LocalAddr().String(),
		PublicationWindowBytes: 64,
		Metrics:                pubMetrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	firstErr := make(chan error, 1)
	go func() {
		firstErr <- pub.Send(ctx, []byte("payload"))
	}()

	select {
	case <-received:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer waitCancel()
	if err := pub.Send(waitCtx, []byte("payload")); !errors.Is(err, ErrBackPressure) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second send err = %v, want back pressure deadline", err)
	}

	close(releaseHandler)
	select {
	case err := <-firstErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	snapshot := pubMetrics.Snapshot()
	if snapshot.BackPressureEvents != 1 || snapshot.SendErrors != 1 || snapshot.MessagesSent != 1 {
		t.Fatalf("unexpected publication metrics: %#v", snapshot)
	}
}
