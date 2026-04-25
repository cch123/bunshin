package bunshin

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMediaDriverManagesPublicationSubscription(t *testing.T) {
	metrics := &Metrics{}
	events := make(chan LogEvent, 32)
	logger := LoggerFunc(func(_ context.Context, event LogEvent) {
		events <- event
	})
	driver, err := StartMediaDriver(DriverConfig{
		Metrics: metrics,
		Logger:  logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := driver.NewClient(ctx, "test-client")
	if err != nil {
		t.Fatal(err)
	}

	sub, err := client.AddSubscription(ctx, SubscriptionConfig{
		StreamID:  120,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan Message, 1)
	go func() {
		_ = sub.Serve(ctx, func(_ context.Context, msg Message) error {
			received <- msg
			return nil
		})
	}()

	pub, err := client.AddPublication(ctx, PublicationConfig{
		StreamID:   120,
		SessionID:  220,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := pub.Send(ctx, []byte("driver payload")); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-received:
		if msg.StreamID != 120 || msg.SessionID != 220 || string(msg.Payload) != "driver payload" {
			t.Fatalf("unexpected message: %#v", msg)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	snapshot, err := driver.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Clients) != 1 || len(snapshot.Publications) != 1 || len(snapshot.Subscriptions) != 1 {
		t.Fatalf("unexpected snapshot: %#v", snapshot)
	}
	if snapshot.Clients[0].Name != "test-client" || snapshot.Clients[0].Publications != 1 || snapshot.Clients[0].Subscriptions != 1 {
		t.Fatalf("unexpected client snapshot: %#v", snapshot.Clients[0])
	}
	if snapshot.Publications[0].StreamID != 120 || snapshot.Publications[0].SessionID != 220 || snapshot.Subscriptions[0].StreamID != 120 {
		t.Fatalf("unexpected resource snapshots: %#v %#v", snapshot.Publications[0], snapshot.Subscriptions[0])
	}
	if snapshot.Counters.ClientsRegistered != 1 || snapshot.Counters.PublicationsRegistered != 1 || snapshot.Counters.SubscriptionsRegistered != 1 {
		t.Fatalf("unexpected counters: %#v", snapshot.Counters)
	}
	if metrics.Snapshot().MessagesSent != 1 {
		t.Fatalf("driver metrics were not inherited: %#v", metrics.Snapshot())
	}
	waitForDriverEvents(t, events,
		driverEventKey("client", "client registered"),
		driverEventKey("publication", "publication registered"),
		driverEventKey("subscription", "subscription registered"),
	)
}

func TestMediaDriverClientCloseClosesResources(t *testing.T) {
	driver, err := StartMediaDriver(DriverConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := driver.NewClient(ctx, "close-client")
	if err != nil {
		t.Fatal(err)
	}
	sub, err := client.AddSubscription(ctx, SubscriptionConfig{
		StreamID:  121,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = sub.Serve(ctx, func(context.Context, Message) error {
			return nil
		})
	}()
	pub, err := client.AddPublication(ctx, PublicationConfig{
		StreamID:   121,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pub.Send(ctx, []byte("closed")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Send() err = %v, want %v", err, ErrClosed)
	}

	snapshot, err := driver.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Clients) != 0 || len(snapshot.Publications) != 0 || len(snapshot.Subscriptions) != 0 {
		t.Fatalf("resources were not closed: %#v", snapshot)
	}
	if snapshot.Counters.ClientsClosed != 1 || snapshot.Counters.PublicationsClosed != 1 || snapshot.Counters.SubscriptionsClosed != 1 {
		t.Fatalf("unexpected close counters: %#v", snapshot.Counters)
	}
}

func TestMediaDriverCleanupClosesStaleClients(t *testing.T) {
	driver, err := StartMediaDriver(DriverConfig{
		ClientTimeout:   25 * time.Millisecond,
		CleanupInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := driver.NewClient(ctx, "stale-client")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.AddSubscription(ctx, SubscriptionConfig{
		StreamID:  122,
		LocalAddr: "127.0.0.1:0",
	}); err != nil {
		t.Fatal(err)
	}

	var snapshot DriverSnapshot
	waitForDriverSnapshot(t, driver, ctx, func(got DriverSnapshot) bool {
		snapshot = got
		return len(got.Clients) == 0 && len(got.Subscriptions) == 0 && got.Counters.StaleClientsClosed == 1
	})
	if snapshot.Counters.ClientsClosed != 1 || snapshot.Counters.SubscriptionsClosed != 1 || snapshot.Counters.CleanupRuns == 0 {
		t.Fatalf("unexpected cleanup snapshot: %#v", snapshot)
	}
	if err := client.Heartbeat(ctx); !errors.Is(err, ErrDriverClientClosed) {
		t.Fatalf("Heartbeat() err = %v, want %v", err, ErrDriverClientClosed)
	}
}

func TestMediaDriverHeartbeatKeepsClientActive(t *testing.T) {
	driver, err := StartMediaDriver(DriverConfig{
		ClientTimeout:   40 * time.Millisecond,
		CleanupInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := driver.NewClient(ctx, "heartbeat-client")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Heartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if err := client.Heartbeat(ctx); err != nil {
		t.Fatal(err)
	}

	snapshot, err := driver.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Clients) != 1 {
		t.Fatalf("client was cleaned up too early: %#v", snapshot)
	}
}

func TestMediaDriverRejectsCommandsAfterClose(t *testing.T) {
	driver, err := StartMediaDriver(DriverConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := driver.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.NewClient(context.Background(), "closed"); !errors.Is(err, ErrDriverClosed) {
		t.Fatalf("NewClient() err = %v, want %v", err, ErrDriverClosed)
	}
}

func waitForDriverSnapshot(t *testing.T, driver *MediaDriver, ctx context.Context, done func(DriverSnapshot) bool) {
	t.Helper()

	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			snapshot, err := driver.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if done(snapshot) {
				return
			}
		case <-deadline.C:
			snapshot, _ := driver.Snapshot(context.Background())
			t.Fatalf("timed out waiting for driver snapshot: %#v", snapshot)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

func waitForDriverEvents(t *testing.T, events <-chan LogEvent, keys ...string) {
	t.Helper()

	needed := make(map[string]bool, len(keys))
	for _, key := range keys {
		needed[key] = false
	}
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		allFound := true
		for _, found := range needed {
			if !found {
				allFound = false
				break
			}
		}
		if allFound {
			return
		}

		select {
		case event := <-events:
			if event.Component == "media_driver" {
				key := driverEventKey(event.Operation, event.Message)
				if _, ok := needed[key]; ok {
					needed[key] = true
				}
			}
		case <-deadline.C:
			t.Fatalf("timed out waiting for driver events: %#v", needed)
		}
	}
}

func driverEventKey(operation, message string) string {
	return operation + "\x00" + message
}
