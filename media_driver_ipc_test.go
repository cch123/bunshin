package bunshin

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDriverIPCRoundTripCommandsAndEvents(t *testing.T) {
	root := t.TempDir()
	clientIPC, err := OpenDriverIPC(DriverIPCConfig{
		Directory:           root,
		CommandRingCapacity: 4096,
		EventRingCapacity:   4096,
		Reset:               true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clientIPC.Close()
	serverIPC, err := OpenDriverIPC(DriverIPCConfig{Directory: root})
	if err != nil {
		t.Fatal(err)
	}
	defer serverIPC.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	correlationID, err := clientIPC.SendCommand(ctx, DriverIPCCommand{
		Type:       DriverIPCCommandOpenClient,
		ClientName: "ipc-client",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var command DriverIPCCommand
	n, err := serverIPC.PollCommands(1, func(got DriverIPCCommand) error {
		command = got
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || command.CorrelationID != correlationID ||
		command.Type != DriverIPCCommandOpenClient ||
		command.ClientName != "ipc-client" {
		t.Fatalf("unexpected command n=%d command=%#v correlation=%d", n, command, correlationID)
	}
	if err := serverIPC.SendEvent(ctx, DriverIPCEvent{
		CorrelationID: correlationID,
		Type:          DriverIPCEventClientOpened,
		CommandType:   command.Type,
		ClientID:      7,
		Message:       command.ClientName,
	}, nil); err != nil {
		t.Fatal(err)
	}
	event := pollDriverIPCEvent(t, clientIPC)
	if event.CorrelationID != correlationID ||
		event.Type != DriverIPCEventClientOpened ||
		event.ClientID != 7 ||
		event.Message != "ipc-client" ||
		event.At.IsZero() {
		t.Fatalf("unexpected event: %#v", event)
	}
}

func TestDriverIPCServerHandlesClientLifecycleAndErrors(t *testing.T) {
	root := t.TempDir()
	driver, err := StartMediaDriver(DriverConfig{
		Directory:       root,
		CleanupInterval: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()

	clientIPC, server := openDriverIPCServerPair(t, root, driver)
	defer clientIPC.Close()
	defer server.ipc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	openCorrelation, err := clientIPC.SendCommand(ctx, DriverIPCCommand{
		Type:       DriverIPCCommandOpenClient,
		ClientName: "managed",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.Poll(ctx, 1); err != nil {
		t.Fatal(err)
	}
	opened := pollDriverIPCEvent(t, clientIPC)
	if opened.Type != DriverIPCEventClientOpened ||
		opened.CorrelationID != openCorrelation ||
		opened.ClientID == 0 ||
		opened.Message != "managed" {
		t.Fatalf("unexpected open event: %#v", opened)
	}

	heartbeatCorrelation, err := clientIPC.SendCommand(ctx, DriverIPCCommand{
		Type:     DriverIPCCommandHeartbeatClient,
		ClientID: opened.ClientID,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.Poll(ctx, 1); err != nil {
		t.Fatal(err)
	}
	heartbeat := pollDriverIPCEvent(t, clientIPC)
	if heartbeat.Type != DriverIPCEventClientHeartbeat ||
		heartbeat.CorrelationID != heartbeatCorrelation ||
		heartbeat.ClientID != opened.ClientID {
		t.Fatalf("unexpected heartbeat event: %#v", heartbeat)
	}

	snapshotCorrelation, err := clientIPC.SendCommand(ctx, DriverIPCCommand{Type: DriverIPCCommandSnapshot}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.Poll(ctx, 1); err != nil {
		t.Fatal(err)
	}
	snapshot := pollDriverIPCEvent(t, clientIPC)
	if snapshot.Type != DriverIPCEventSnapshot ||
		snapshot.CorrelationID != snapshotCorrelation ||
		snapshot.Snapshot == nil ||
		len(snapshot.Snapshot.Clients) != 1 {
		t.Fatalf("unexpected snapshot event: %#v", snapshot)
	}

	errorCorrelation, err := clientIPC.SendCommand(ctx, DriverIPCCommand{
		Type:     DriverIPCCommandHeartbeatClient,
		ClientID: 999,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.Poll(ctx, 1); err != nil {
		t.Fatal(err)
	}
	failed := pollDriverIPCEvent(t, clientIPC)
	if failed.Type != DriverIPCEventCommandError ||
		failed.CorrelationID != errorCorrelation ||
		!strings.Contains(failed.Error, ErrDriverClientClosed.Error()) {
		t.Fatalf("unexpected error event: %#v", failed)
	}
	if err := driverIPCCommandError(failed.Error); !errors.Is(err, ErrDriverClientClosed) {
		t.Fatalf("driverIPCCommandError() err = %v, want %v", err, ErrDriverClientClosed)
	}
}

func TestDriverIPCServerRoutesEventsToResponseRings(t *testing.T) {
	root := t.TempDir()
	driver, err := StartMediaDriver(DriverConfig{
		Directory:       root,
		CleanupInterval: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()

	clientOneIPC, err := OpenDriverIPC(DriverIPCConfig{
		Directory:           root,
		EventRingPath:       filepath.Join(root, "clients", "client-one-events.ring"),
		CommandRingCapacity: 4096,
		EventRingCapacity:   4096,
		Reset:               true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clientOneIPC.Close()
	clientTwoIPC, err := OpenDriverIPC(DriverIPCConfig{
		Directory:         root,
		EventRingPath:     filepath.Join(root, "clients", "client-two-events.ring"),
		EventRingCapacity: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clientTwoIPC.Close()
	serverIPC, err := OpenDriverIPC(DriverIPCConfig{Directory: root})
	if err != nil {
		t.Fatal(err)
	}
	defer serverIPC.Close()
	server, err := NewDriverIPCServer(driver, serverIPC)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	correlationOne, err := clientOneIPC.SendCommand(ctx, DriverIPCCommand{
		Type:       DriverIPCCommandOpenClient,
		ClientName: "client-one",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	correlationTwo, err := clientTwoIPC.SendCommand(ctx, DriverIPCCommand{
		Type:       DriverIPCCommandOpenClient,
		ClientName: "client-two",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.Poll(ctx, 2); err != nil {
		t.Fatal(err)
	}

	eventTwo, err := waitDriverIPCEvent(ctx, clientTwoIPC, correlationTwo)
	if err != nil {
		t.Fatal(err)
	}
	if eventTwo.Type != DriverIPCEventClientOpened || eventTwo.Message != "client-two" {
		t.Fatalf("unexpected client two event: %#v", eventTwo)
	}
	eventOne, err := waitDriverIPCEvent(ctx, clientOneIPC, correlationOne)
	if err != nil {
		t.Fatal(err)
	}
	if eventOne.Type != DriverIPCEventClientOpened || eventOne.Message != "client-one" {
		t.Fatalf("unexpected client one event: %#v", eventOne)
	}
}

func TestDriverIPCServerHandlesSubscriptionsAndReportFlush(t *testing.T) {
	root := t.TempDir()
	driver, err := StartMediaDriver(DriverConfig{
		Directory:       root,
		CleanupInterval: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()

	clientIPC, server := openDriverIPCServerPair(t, root, driver)
	defer clientIPC.Close()
	defer server.ipc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := clientIPC.SendCommand(ctx, DriverIPCCommand{Type: DriverIPCCommandOpenClient}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Poll(ctx, 1); err != nil {
		t.Fatal(err)
	}
	opened := pollDriverIPCEvent(t, clientIPC)

	if _, err := clientIPC.SendCommand(ctx, DriverIPCCommand{
		Type:     DriverIPCCommandAddSubscription,
		ClientID: opened.ClientID,
		Subscription: DriverIPCSubscriptionConfig{
			StreamID:         55,
			LocalAddr:        "127.0.0.1:0",
			DataRingCapacity: 4096,
		},
	}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Poll(ctx, 1); err != nil {
		t.Fatal(err)
	}
	added := pollDriverIPCEvent(t, clientIPC)
	if added.Type != DriverIPCEventSubscriptionAdded ||
		added.ClientID != opened.ClientID ||
		added.ResourceID == 0 ||
		added.DataRingPath == "" ||
		!strings.HasPrefix(added.Message, "127.0.0.1:") {
		t.Fatalf("unexpected subscription event: %#v", added)
	}

	snapshotCorrelation, err := clientIPC.SendCommand(ctx, DriverIPCCommand{Type: DriverIPCCommandSnapshot}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.Poll(ctx, 1); err != nil {
		t.Fatal(err)
	}
	snapshot := pollDriverIPCEvent(t, clientIPC)
	if snapshot.Type != DriverIPCEventSnapshot ||
		snapshot.CorrelationID != snapshotCorrelation ||
		snapshot.Snapshot == nil ||
		len(snapshot.Snapshot.Subscriptions) != 1 ||
		snapshot.Snapshot.Subscriptions[0].DataRingPath != added.DataRingPath ||
		!snapshot.Snapshot.Subscriptions[0].DataRingMapped ||
		snapshot.Snapshot.Subscriptions[0].DataRingCapacity != 4096 ||
		snapshot.Snapshot.Subscriptions[0].DataRingFree == 0 {
		t.Fatalf("unexpected subscription data ring snapshot: %#v", snapshot)
	}

	pollCorrelation, err := clientIPC.SendCommand(ctx, DriverIPCCommand{
		Type:          DriverIPCCommandPollSubscription,
		ClientID:      opened.ClientID,
		ResourceID:    added.ResourceID,
		MessageLimit:  1,
		FragmentLimit: 1,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.Poll(ctx, 1); err != nil {
		t.Fatal(err)
	}
	polled := pollDriverIPCEvent(t, clientIPC)
	if polled.Type != DriverIPCEventSubscriptionPolled ||
		polled.CorrelationID != pollCorrelation ||
		polled.ResourceID != added.ResourceID ||
		len(polled.Messages) != 0 {
		t.Fatalf("unexpected empty poll event: %#v", polled)
	}

	if _, err := clientIPC.SendCommand(ctx, DriverIPCCommand{
		Type:          DriverIPCCommandFlushReports,
		CorrelationID: 100,
	}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Poll(ctx, 1); err != nil {
		t.Fatal(err)
	}
	flushed := pollDriverIPCEvent(t, clientIPC)
	if flushed.Type != DriverIPCEventReportsFlushed ||
		flushed.CorrelationID != 100 ||
		flushed.Directory == nil ||
		flushed.Directory.Counters.Subscriptions != 1 ||
		len(flushed.Directory.Rings.Subscriptions) != 1 ||
		flushed.Directory.Rings.Subscriptions[0].Path != added.DataRingPath ||
		flushed.Directory.Rings.Subscriptions[0].Capacity != 4096 {
		t.Fatalf("unexpected flush event: %#v", flushed)
	}
	layout, err := ResolveDriverDirectoryLayout(root)
	if err != nil {
		t.Fatal(err)
	}
	rings := readDriverJSON[DriverRingsReportFile](t, layout.RingsReportFile)
	if len(rings.Subscriptions) != 1 ||
		rings.Subscriptions[0].Path != added.DataRingPath ||
		rings.Subscriptions[0].Capacity != 4096 {
		t.Fatalf("unexpected rings report file: %#v", rings)
	}

	if _, err := clientIPC.SendCommand(ctx, DriverIPCCommand{
		Type:       DriverIPCCommandCloseSubscription,
		ClientID:   opened.ClientID,
		ResourceID: added.ResourceID,
	}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Poll(ctx, 1); err != nil {
		t.Fatal(err)
	}
	closed := pollDriverIPCEvent(t, clientIPC)
	if closed.Type != DriverIPCEventSubscriptionClosed ||
		closed.ResourceID != added.ResourceID {
		t.Fatalf("unexpected close subscription event: %#v", closed)
	}
}

func TestDriverIPCSubscriptionPollReportsDataRingBackPressure(t *testing.T) {
	root := t.TempDir()
	metrics := &Metrics{}
	driver, err := StartMediaDriver(DriverConfig{
		Directory:       root,
		CleanupInterval: time.Second,
		Metrics:         metrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()

	clientIPC, server := openDriverIPCServerPair(t, root, driver)
	defer clientIPC.Close()
	defer server.Close()
	defer server.ipc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := clientIPC.SendCommand(ctx, DriverIPCCommand{Type: DriverIPCCommandOpenClient}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Poll(ctx, 1); err != nil {
		t.Fatal(err)
	}
	opened := pollDriverIPCEvent(t, clientIPC)

	if _, err := clientIPC.SendCommand(ctx, DriverIPCCommand{
		Type:     DriverIPCCommandAddSubscription,
		ClientID: opened.ClientID,
		Subscription: DriverIPCSubscriptionConfig{
			StreamID:         56,
			LocalAddr:        "127.0.0.1:0",
			DataRingCapacity: 4096,
		},
	}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Poll(ctx, 1); err != nil {
		t.Fatal(err)
	}
	added := pollDriverIPCEvent(t, clientIPC)
	if added.Type != DriverIPCEventSubscriptionAdded || added.ResourceID == 0 {
		t.Fatalf("unexpected subscription event: %#v", added)
	}

	spy, err := ListenSubscription(SubscriptionConfig{
		StreamID:  56,
		LocalAddr: "driver-spy",
		LocalSpy:  true,
		Metrics:   metrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer spy.Close()
	if owned := server.subscriptions[added.ResourceID]; owned == nil || owned.subscription == nil {
		t.Fatalf("server did not retain subscription %d", added.ResourceID)
	} else {
		_ = owned.subscription.Close()
		owned.subscription = spy
	}

	dataRing := server.dataRings[added.ResourceID]
	if dataRing == nil {
		t.Fatalf("server did not create data ring for subscription %d", added.ResourceID)
	}
	fillIPCRingForTest(t, dataRing, []byte("pad"))
	spy.localSpyCh <- Message{
		StreamID:  56,
		SessionID: 66,
		Payload:   []byte("back pressured payload"),
	}

	pollCorrelation, err := clientIPC.SendCommand(ctx, DriverIPCCommand{
		Type:          DriverIPCCommandPollSubscription,
		ClientID:      opened.ClientID,
		ResourceID:    added.ResourceID,
		MessageLimit:  1,
		FragmentLimit: 1,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.Poll(ctx, 1); err != nil {
		t.Fatal(err)
	}
	polled := pollDriverIPCEvent(t, clientIPC)
	if polled.Type != DriverIPCEventSubscriptionPolled ||
		polled.CorrelationID != pollCorrelation ||
		polled.ResourceID != added.ResourceID ||
		!polled.BackPressured ||
		polled.MessageCount != 0 ||
		len(polled.Messages) != 0 ||
		polled.DataRingPath != added.DataRingPath ||
		polled.DataRingCapacity != 4096 ||
		polled.DataRingUsed == 0 {
		t.Fatalf("unexpected back-pressured poll event: %#v", polled)
	}
	if snapshot := metrics.Snapshot(); snapshot.BackPressureEvents != 1 || snapshot.ReceiveErrors != 0 {
		t.Fatalf("unexpected metrics after data ring back pressure: %#v", snapshot)
	}
	select {
	case msg := <-spy.localSpyCh:
		if string(msg.Payload) != "back pressured payload" {
			t.Fatalf("unexpected retained message after preflight back pressure: %#v", msg)
		}
	default:
		t.Fatal("poll consumed a message after data ring preflight back pressure")
	}
}

func TestDriverIPCSubscriptionPollFallsBackWhenMessageExceedsDataRing(t *testing.T) {
	root := t.TempDir()
	metrics := &Metrics{}
	driver, err := StartMediaDriver(DriverConfig{
		Directory:       root,
		CleanupInterval: time.Second,
		Metrics:         metrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()

	clientIPC, server := openDriverIPCServerPair(t, root, driver)
	defer clientIPC.Close()
	defer server.Close()
	defer server.ipc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := clientIPC.SendCommand(ctx, DriverIPCCommand{Type: DriverIPCCommandOpenClient}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Poll(ctx, 1); err != nil {
		t.Fatal(err)
	}
	opened := pollDriverIPCEvent(t, clientIPC)

	if _, err := clientIPC.SendCommand(ctx, DriverIPCCommand{
		Type:     DriverIPCCommandAddSubscription,
		ClientID: opened.ClientID,
		Subscription: DriverIPCSubscriptionConfig{
			StreamID:         57,
			LocalAddr:        "127.0.0.1:0",
			DataRingCapacity: 4096,
		},
	}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Poll(ctx, 1); err != nil {
		t.Fatal(err)
	}
	added := pollDriverIPCEvent(t, clientIPC)
	if added.Type != DriverIPCEventSubscriptionAdded || added.ResourceID == 0 {
		t.Fatalf("unexpected subscription event: %#v", added)
	}

	spy, err := ListenSubscription(SubscriptionConfig{
		StreamID:  57,
		LocalAddr: "driver-large-spy",
		LocalSpy:  true,
		Metrics:   metrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer spy.Close()
	if owned := server.subscriptions[added.ResourceID]; owned == nil || owned.subscription == nil {
		t.Fatalf("server did not retain subscription %d", added.ResourceID)
	} else {
		_ = owned.subscription.Close()
		owned.subscription = spy
	}

	payload := bytes.Repeat([]byte("x"), 5000)
	spy.localSpyCh <- Message{
		StreamID:  57,
		SessionID: 67,
		Payload:   payload,
	}
	pollCorrelation, err := clientIPC.SendCommand(ctx, DriverIPCCommand{
		Type:          DriverIPCCommandPollSubscription,
		ClientID:      opened.ClientID,
		ResourceID:    added.ResourceID,
		MessageLimit:  1,
		FragmentLimit: 1,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.Poll(ctx, 1); err != nil {
		t.Fatal(err)
	}
	polled := pollDriverIPCEvent(t, clientIPC)
	if polled.Type != DriverIPCEventSubscriptionPolled ||
		polled.CorrelationID != pollCorrelation ||
		polled.ResourceID != added.ResourceID ||
		!polled.BackPressured ||
		polled.MessageCount != 0 ||
		len(polled.Messages) != 1 ||
		!bytes.Equal(polled.Messages[0].Payload, payload) {
		t.Fatalf("unexpected fallback poll event: %#v", polled)
	}
	if snapshot := metrics.Snapshot(); snapshot.BackPressureEvents != 1 || snapshot.ReceiveErrors != 0 || snapshot.MessagesReceived != 1 {
		t.Fatalf("unexpected metrics after fallback data ring back pressure: %#v", snapshot)
	}
}

func TestDriverIPCSubscriptionPumpQueuesFallbackWhenMessageExceedsDataRing(t *testing.T) {
	root := t.TempDir()
	metrics := &Metrics{}
	driver, err := StartMediaDriver(DriverConfig{
		Directory:       root,
		CleanupInterval: time.Second,
		Metrics:         metrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()

	clientIPC, server := openDriverIPCServerPair(t, root, driver)
	defer clientIPC.Close()
	defer server.Close()
	defer server.ipc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := clientIPC.SendCommand(ctx, DriverIPCCommand{Type: DriverIPCCommandOpenClient}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Poll(ctx, 1); err != nil {
		t.Fatal(err)
	}
	opened := pollDriverIPCEvent(t, clientIPC)

	if _, err := clientIPC.SendCommand(ctx, DriverIPCCommand{
		Type:     DriverIPCCommandAddSubscription,
		ClientID: opened.ClientID,
		Subscription: DriverIPCSubscriptionConfig{
			StreamID:         58,
			LocalAddr:        "127.0.0.1:0",
			DataRingCapacity: 4096,
		},
	}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Poll(ctx, 1); err != nil {
		t.Fatal(err)
	}
	added := pollDriverIPCEvent(t, clientIPC)
	if added.Type != DriverIPCEventSubscriptionAdded || added.ResourceID == 0 {
		t.Fatalf("unexpected subscription event: %#v", added)
	}

	spy, err := ListenSubscription(SubscriptionConfig{
		StreamID:  58,
		LocalAddr: "driver-pump-large-spy",
		LocalSpy:  true,
		Metrics:   metrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer spy.Close()
	if owned := server.subscriptions[added.ResourceID]; owned == nil || owned.subscription == nil {
		t.Fatalf("server did not retain subscription %d", added.ResourceID)
	} else {
		_ = owned.subscription.Close()
		owned.subscription = spy
	}

	payload := bytes.Repeat([]byte("z"), 5000)
	spy.localSpyCh <- Message{
		StreamID:  58,
		SessionID: 68,
		Payload:   payload,
	}
	work, err := server.PumpSubscriptions(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if work != 1 || server.subscriptionFallbackCount(added.ResourceID) != 1 {
		t.Fatalf("pump did not queue fallback work=%d pending=%d", work, server.subscriptionFallbackCount(added.ResourceID))
	}
	snapshot, err := server.snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Subscriptions) != 1 || snapshot.Subscriptions[0].DataRingPendingMessages != 1 {
		t.Fatalf("snapshot did not report pending fallback: %#v", snapshot.Subscriptions)
	}

	pollCorrelation, err := clientIPC.SendCommand(ctx, DriverIPCCommand{
		Type:          DriverIPCCommandPollSubscription,
		ClientID:      opened.ClientID,
		ResourceID:    added.ResourceID,
		MessageLimit:  1,
		FragmentLimit: 1,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.Poll(ctx, 1); err != nil {
		t.Fatal(err)
	}
	polled := pollDriverIPCEvent(t, clientIPC)
	if polled.Type != DriverIPCEventSubscriptionPolled ||
		polled.CorrelationID != pollCorrelation ||
		polled.ResourceID != added.ResourceID ||
		!polled.BackPressured ||
		polled.MessageCount != 0 ||
		len(polled.Messages) != 1 ||
		!bytes.Equal(polled.Messages[0].Payload, payload) {
		t.Fatalf("unexpected fallback poll event after pump: %#v", polled)
	}
	if pending := server.subscriptionFallbackCount(added.ResourceID); pending != 0 {
		t.Fatalf("fallback remained pending after poll: %d", pending)
	}
	if snapshot := metrics.Snapshot(); snapshot.BackPressureEvents != 1 || snapshot.ReceiveErrors != 0 || snapshot.MessagesReceived != 1 {
		t.Fatalf("unexpected metrics after pump fallback: %#v", snapshot)
	}
}

func TestDriverIPCRejectsMalformedProtocol(t *testing.T) {
	root := t.TempDir()
	ipc, err := OpenDriverIPC(DriverIPCConfig{
		Directory:           root,
		CommandRingCapacity: 4096,
		EventRingCapacity:   4096,
		Reset:               true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ipc.Close()

	if err := ipc.commandRing.Offer([]byte(`{"version":99,"correlation_id":1,"type":"snapshot"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := ipc.PollCommands(1, func(DriverIPCCommand) error {
		t.Fatal("malformed command should not be delivered")
		return nil
	}); !errors.Is(err, ErrDriverIPCProtocol) {
		t.Fatalf("PollCommands() err = %v, want %v", err, ErrDriverIPCProtocol)
	}
}

func openDriverIPCServerPair(t *testing.T, root string, driver *MediaDriver) (*DriverIPC, *DriverIPCServer) {
	t.Helper()
	clientIPC, err := OpenDriverIPC(DriverIPCConfig{
		Directory:           root,
		CommandRingCapacity: 64 * 1024,
		EventRingCapacity:   64 * 1024,
		Reset:               true,
	})
	if err != nil {
		t.Fatal(err)
	}
	serverIPC, err := OpenDriverIPC(DriverIPCConfig{Directory: root})
	if err != nil {
		_ = clientIPC.Close()
		t.Fatal(err)
	}
	server, err := NewDriverIPCServer(driver, serverIPC)
	if err != nil {
		_ = clientIPC.Close()
		_ = serverIPC.Close()
		t.Fatal(err)
	}
	return clientIPC, server
}

func pollDriverIPCEvent(t *testing.T, ipc *DriverIPC) DriverIPCEvent {
	t.Helper()
	var event DriverIPCEvent
	n, err := ipc.PollEvents(1, func(got DriverIPCEvent) error {
		event = got
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("PollEvents() n = %d, want 1", n)
	}
	return event
}

func fillIPCRingForTest(t *testing.T, ring *IPCRing, payload []byte) {
	t.Helper()
	for {
		err := ring.Offer(payload)
		if err == nil {
			continue
		}
		if errors.Is(err, ErrIPCRingFull) {
			return
		}
		t.Fatal(err)
	}
}
