package bunshin

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestExternalDriverClientPublishesThroughDriverOwnedResource(t *testing.T) {
	root := t.TempDir()
	done := startExternalDriverProcessForTest(t, root)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := ConnectMediaDriver(ctx, DriverConnectionConfig{
		Directory:  root,
		ClientName: "external-publisher",
		Timeout:    time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	sub, err := ListenSubscription(SubscriptionConfig{
		StreamID:  240,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	received := make(chan Message, 1)
	go func() {
		_ = sub.Serve(ctx, func(_ context.Context, msg Message) error {
			received <- msg
			return nil
		})
	}()

	pub, err := client.AddPublication(ctx, PublicationConfig{
		StreamID:   240,
		SessionID:  340,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if pub.ID() == 0 || pub.LocalAddr() == nil {
		t.Fatalf("unexpected external publication handle: %#v local=%v", pub, pub.LocalAddr())
	}
	if err := pub.Send(ctx, []byte("external payload")); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-received:
		if msg.StreamID != 240 || msg.SessionID != 340 || string(msg.Payload) != "external payload" {
			t.Fatalf("unexpected message: %#v", msg)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	snapshot, err := client.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Clients) != 1 || len(snapshot.Publications) != 1 ||
		snapshot.Publications[0].ID != pub.ID() ||
		snapshot.Publications[0].ClientID != client.ID() {
		t.Fatalf("unexpected external driver snapshot: %#v", snapshot)
	}
	if err := pub.Close(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err = client.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Publications) != 0 || snapshot.Counters.PublicationsClosed != 1 {
		t.Fatalf("publication remained owned by driver after close: %#v", snapshot)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}

	terminateExternalDriverProcessForTest(t, root, done)
}

func TestExternalDriverClientOwnsSubscriptionHandle(t *testing.T) {
	root := t.TempDir()
	done := startExternalDriverProcessForTest(t, root)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := ConnectMediaDriver(ctx, DriverConnectionConfig{
		Directory:  root,
		ClientName: "external-subscriber",
		Timeout:    time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	sub, err := client.AddSubscription(ctx, SubscriptionConfig{
		StreamID:  241,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if sub.ID() == 0 || sub.LocalAddr() == nil || sub.LocalAddr().String() == "" {
		t.Fatalf("unexpected external subscription handle: %#v local=%v", sub, sub.LocalAddr())
	}
	if err := sub.Serve(ctx, func(context.Context, Message) error { return nil }); !errors.Is(err, ErrDriverExternalUnsupported) {
		t.Fatalf("Serve() err = %v, want %v", err, ErrDriverExternalUnsupported)
	}
	snapshot, err := client.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Subscriptions) != 1 || snapshot.Subscriptions[0].ID != sub.ID() ||
		snapshot.Subscriptions[0].ClientID != client.ID() {
		t.Fatalf("unexpected subscription snapshot: %#v", snapshot)
	}
	if err := sub.Close(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err = client.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Subscriptions) != 0 || snapshot.Counters.SubscriptionsClosed != 1 {
		t.Fatalf("subscription remained owned by driver after close: %#v", snapshot)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}

	terminateExternalDriverProcessForTest(t, root, done)
}

func startExternalDriverProcessForTest(t *testing.T, root string) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		done <- RunMediaDriverProcess(context.Background(), DriverProcessConfig{
			Directory:           root,
			ResetIPC:            true,
			CommandRingCapacity: 64 * 1024,
			EventRingCapacity:   64 * 1024,
			HeartbeatInterval:   20 * time.Millisecond,
			IdleStrategy:        SleepingIdleStrategy{Duration: time.Millisecond},
		})
	}()
	layout := waitForDriverProcessStatus(t, root, func(status DriverProcessStatus) bool {
		return status.Active && !status.Stale
	}).Layout
	waitForPath(t, layout.CommandRingFile)
	waitForPath(t, layout.EventRingFile)
	select {
	case err := <-done:
		t.Fatalf("driver process exited early: %v", err)
	default:
	}
	return done
}

func terminateExternalDriverProcessForTest(t *testing.T, root string, done <-chan error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	event, err := TerminateDriverProcess(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != DriverIPCEventTerminated {
		t.Fatalf("unexpected terminate event: %#v", event)
	}
	if err := <-done; err != nil {
		t.Fatalf("RunMediaDriverProcess() err = %v, want nil", err)
	}
}
