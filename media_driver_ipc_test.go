package bunshin

import (
	"context"
	"errors"
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
			StreamID:  55,
			LocalAddr: "127.0.0.1:0",
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
		!strings.HasPrefix(added.Message, "127.0.0.1:") {
		t.Fatalf("unexpected subscription event: %#v", added)
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
		flushed.Directory.Counters.Subscriptions != 1 {
		t.Fatalf("unexpected flush event: %#v", flushed)
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
