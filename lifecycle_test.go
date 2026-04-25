package bunshin

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPublicationSendContextDeadlineCancelsAckWait(t *testing.T) {
	pubMetrics := &Metrics{}
	sub, err := ListenSubscription(SubscriptionConfig{
		StreamID:  106,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	serveCtx, serveCancel := context.WithTimeout(context.Background(), time.Second)
	defer serveCancel()

	received := make(chan struct{}, 1)
	releaseHandler := make(chan struct{})
	go func() {
		_ = sub.Serve(serveCtx, func(ctx context.Context, _ Message) error {
			received <- struct{}{}
			select {
			case <-releaseHandler:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})
	}()

	pub, err := DialPublication(PublicationConfig{
		StreamID:   106,
		RemoteAddr: sub.LocalAddr().String(),
		Metrics:    pubMetrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	sendCtx, sendCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer sendCancel()
	sendErr := make(chan error, 1)
	go func() {
		sendErr <- pub.Send(sendCtx, []byte("payload"))
	}()

	select {
	case <-received:
	case <-serveCtx.Done():
		t.Fatal(serveCtx.Err())
	}
	select {
	case err := <-sendErr:
		close(releaseHandler)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Send() err = %v, want deadline exceeded", err)
		}
	case <-serveCtx.Done():
		close(releaseHandler)
		t.Fatal(serveCtx.Err())
	}

	snapshot := pubMetrics.Snapshot()
	if snapshot.SendErrors != 1 || snapshot.MessagesSent != 0 {
		t.Fatalf("unexpected publication metrics: %#v", snapshot)
	}
}

func TestPublicationSendAfterCloseReturnsErrClosed(t *testing.T) {
	pubMetrics := &Metrics{}
	sub, err := ListenSubscription(SubscriptionConfig{
		StreamID:  107,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	serveCtx, serveCancel := context.WithTimeout(context.Background(), time.Second)
	defer serveCancel()
	go func() {
		_ = sub.Serve(serveCtx, func(context.Context, Message) error {
			return nil
		})
	}()

	pub, err := DialPublication(PublicationConfig{
		StreamID:   107,
		RemoteAddr: sub.LocalAddr().String(),
		Metrics:    pubMetrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := pub.Close(); err != nil {
		t.Fatal(err)
	}
	if err := pub.Close(); err != nil {
		t.Fatal(err)
	}

	err = pub.Send(context.Background(), []byte("payload"))
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("Send() err = %v, want %v", err, ErrClosed)
	}
	if snapshot := pubMetrics.Snapshot(); snapshot.SendErrors != 1 {
		t.Fatalf("send errors = %d, want 1", snapshot.SendErrors)
	}
}

func TestSubscriptionServeAfterCloseReturnsErrClosed(t *testing.T) {
	sub, err := ListenSubscription(SubscriptionConfig{
		StreamID:  108,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = sub.Serve(ctx, func(context.Context, Message) error {
		return nil
	})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("Serve() err = %v, want %v", err, ErrClosed)
	}
}

func TestSubscriptionDuplicateSequenceIsAckedWithoutHandler(t *testing.T) {
	subMetrics := &Metrics{}
	sub, err := ListenSubscription(SubscriptionConfig{
		StreamID:  109,
		LocalAddr: "127.0.0.1:0",
		Metrics:   subMetrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	delivered := make(chan uint64, 2)
	go func() {
		_ = sub.Serve(ctx, func(_ context.Context, msg Message) error {
			delivered <- msg.Sequence
			return nil
		})
	}()

	pub, err := DialPublication(PublicationConfig{
		StreamID:   109,
		SessionID:  210,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	if err := sendDataFrame(ctx, pub, 109, 210, 1, []byte("first")); err != nil {
		t.Fatal(err)
	}
	select {
	case seq := <-delivered:
		if seq != 1 {
			t.Fatalf("delivered sequence = %d, want 1", seq)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	if err := sendDataFrame(ctx, pub, 109, 210, 1, []byte("duplicate")); err != nil {
		t.Fatal(err)
	}
	select {
	case seq := <-delivered:
		t.Fatalf("duplicate sequence delivered to handler: %d", seq)
	case <-time.After(25 * time.Millisecond):
	}

	snapshot := subMetrics.Snapshot()
	if snapshot.MessagesReceived != 1 || snapshot.FramesDropped != 1 {
		t.Fatalf("unexpected duplicate metrics: %#v", snapshot)
	}
}
