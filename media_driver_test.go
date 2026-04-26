package bunshin

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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
	if snapshot.StatusCounters.ActiveClients != 1 || snapshot.StatusCounters.ActivePublications != 1 ||
		snapshot.StatusCounters.ActiveSubscriptions != 1 || snapshot.StatusCounters.Images != 1 ||
		snapshot.StatusCounters.AvailableImages != 1 || snapshot.StatusCounters.ChannelEndpoints != 1 {
		t.Fatalf("unexpected status counters: %#v", snapshot.StatusCounters)
	}
	assertCounterSnapshot(t, snapshot.CounterSnapshots, CounterDriverActivePublications, CounterScopeDriver, "driver_active_publications", 1)
	assertCounterSnapshot(t, snapshot.CounterSnapshots, CounterDriverActiveSubscriptions, CounterScopeDriver, "driver_active_subscriptions", 1)
	assertCounterSnapshot(t, snapshot.CounterSnapshots, CounterDriverAvailableImages, CounterScopeDriver, "driver_available_images", 1)
	assertCounterSnapshot(t, snapshot.CounterSnapshots, CounterMetricsMessagesSent, CounterScopeMetrics, "messages_sent", 1)
	if len(snapshot.Images) != 1 || snapshot.Images[0].ResourceID != sub.ID() ||
		snapshot.Images[0].StreamID != 120 || snapshot.Images[0].SessionID != 220 ||
		snapshot.Images[0].CurrentPosition <= snapshot.Images[0].JoinPosition ||
		snapshot.Images[0].ObservedPosition != snapshot.Images[0].CurrentPosition ||
		snapshot.Images[0].LagBytes != 0 ||
		snapshot.Images[0].LastSequence != 1 || snapshot.Images[0].LastObservedSequence != 1 {
		t.Fatalf("unexpected image snapshots: %#v", snapshot.Images)
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

func TestMediaDriverCreatesMmapTermBuffers(t *testing.T) {
	driver, err := StartMediaDriver(DriverConfig{
		Directory: filepath.Join(t.TempDir(), "driver"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := driver.NewClient(ctx, "mmap-client")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)

	sub, err := client.AddSubscription(ctx, SubscriptionConfig{
		Transport: TransportUDP,
		StreamID:  129,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan struct{}, 1)
	go func() {
		_ = sub.Serve(ctx, func(context.Context, Message) error {
			received <- struct{}{}
			return nil
		})
	}()
	pub, err := client.AddPublication(ctx, PublicationConfig{
		Transport:  TransportUDP,
		StreamID:   129,
		SessionID:  229,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := pub.Send(ctx, []byte("mmap-term-buffer")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-received:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	snapshot, err := driver.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Publications) != 1 || !snapshot.Publications[0].TermBufferMapped ||
		len(snapshot.Publications[0].TermBufferFiles) != termPartitionCount {
		t.Fatalf("unexpected mapped publication snapshot: %#v", snapshot.Publications)
	}
	peerStatuses := snapshot.Subscriptions[0].UDPPeerStatuses
	if len(peerStatuses) != 1 ||
		!peerStatuses[0].Active ||
		peerStatuses[0].DataFramesReceived != 1 ||
		peerStatuses[0].StatusFramesSent != 1 ||
		peerStatuses[0].AckFramesSent != 1 ||
		peerStatuses[0].LastSequence != 1 {
		t.Fatalf("unexpected subscription peer statuses: %#v", peerStatuses)
	}
	destinationStatuses := snapshot.Publications[0].UDPDestinationStatuses
	if len(destinationStatuses) != 1 ||
		!destinationStatuses[0].Active ||
		destinationStatuses[0].SetupFramesReceived != 1 ||
		destinationStatuses[0].StatusFramesReceived != 1 ||
		destinationStatuses[0].AckFramesReceived != 1 ||
		destinationStatuses[0].LastRTT <= 0 {
		t.Fatalf("unexpected publication destination statuses: %#v", destinationStatuses)
	}
	for _, path := range snapshot.Publications[0].TermBufferFiles {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() != minTermLength {
			t.Fatalf("term buffer %s size = %d, want %d", path, info.Size(), minTermLength)
		}
	}
	data, err := os.ReadFile(snapshot.Publications[0].TermBufferFiles[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < len(frameMagic) || string(data[:len(frameMagic)]) != frameMagic {
		t.Fatalf("mapped term buffer does not contain a data frame header: %x", data[:min(len(data), headerLen)])
	}
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

func TestMediaDriverDutyCycleAndStallCounters(t *testing.T) {
	driver, err := StartMediaDriver(DriverConfig{
		StallThreshold: time.Nanosecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := driver.dispatch(ctx, func(*driverState) (any, error) {
		time.Sleep(time.Millisecond)
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}

	snapshot, err := driver.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Counters.DutyCycles == 0 || snapshot.Counters.DutyCycleNanos == 0 ||
		snapshot.Counters.DutyCycleMaxNanos == 0 || snapshot.Counters.Stalls == 0 ||
		snapshot.Counters.StallNanos == 0 || snapshot.Counters.StallMaxNanos == 0 {
		t.Fatalf("unexpected duty-cycle counters: %#v", snapshot.Counters)
	}
	assertCounterSnapshot(t, snapshot.CounterSnapshots, CounterDriverDutyCycles, CounterScopeDriver, "driver_duty_cycles", snapshot.Counters.DutyCycles)
	assertCounterSnapshot(t, snapshot.CounterSnapshots, CounterDriverStalls, CounterScopeDriver, "driver_stalls", snapshot.Counters.Stalls)
}

func TestMediaDriverDedicatedAgentLoops(t *testing.T) {
	driver, err := StartMediaDriver(DriverConfig{
		ThreadingMode:        DriverThreadingDedicated,
		SenderIdleStrategy:   SleepingIdleStrategy{Duration: time.Millisecond},
		ReceiverIdleStrategy: SleepingIdleStrategy{Duration: time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := driver.NewClient(ctx, "agent-client"); err != nil {
		t.Fatal(err)
	}
	waitForDriverSnapshot(t, driver, ctx, func(snapshot DriverSnapshot) bool {
		return snapshot.Counters.ConductorDutyCycles > 0 &&
			snapshot.Counters.SenderDutyCycles > 0 &&
			snapshot.Counters.ReceiverDutyCycles > 0
	})
	snapshot, err := driver.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assertCounterSnapshot(t, snapshot.CounterSnapshots, CounterDriverConductorDutyCycles, CounterScopeDriver, "driver_conductor_duty_cycles", snapshot.Counters.ConductorDutyCycles)
	assertCounterSnapshot(t, snapshot.CounterSnapshots, CounterDriverSenderDutyCycles, CounterScopeDriver, "driver_sender_duty_cycles", snapshot.Counters.SenderDutyCycles)
	assertCounterSnapshot(t, snapshot.CounterSnapshots, CounterDriverReceiverDutyCycles, CounterScopeDriver, "driver_receiver_duty_cycles", snapshot.Counters.ReceiverDutyCycles)
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

func TestMediaDriverRejectsInvalidThreadingMode(t *testing.T) {
	if _, err := StartMediaDriver(DriverConfig{ThreadingMode: DriverThreadingMode("invalid")}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("StartMediaDriver() err = %v, want %v", err, ErrInvalidConfig)
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
