package bunshin

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func TestExternalDriverClientPublishesThroughDriverOwnedResource(t *testing.T) {
	root := t.TempDir()
	done := startExternalDriverProcessForTest(t, root)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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
		StreamID:               241,
		LocalAddr:              "127.0.0.1:0",
		DriverDataRingCapacity: 2 * defaultDriverIPCSubscriptionDataRing,
	})
	if err != nil {
		t.Fatal(err)
	}
	if sub.ID() == 0 || sub.LocalAddr() == nil || sub.LocalAddr().String() == "" {
		t.Fatalf("unexpected external subscription handle: %#v local=%v", sub, sub.LocalAddr())
	}
	if sub.dataRing == nil || sub.dataRingPath == "" {
		t.Fatalf("external subscription did not open a data ring: %#v path=%q", sub.dataRing, sub.dataRingPath)
	}
	if _, err := os.Stat(sub.dataRingPath); err != nil {
		t.Fatalf("stat external subscription data ring: %v", err)
	}
	if err := sub.Serve(ctx, func(context.Context, Message) error { return nil }); !errors.Is(err, ErrDriverExternalUnsupported) {
		t.Fatalf("Serve() err = %v, want %v", err, ErrDriverExternalUnsupported)
	}

	type pollResult struct {
		n        int
		messages []Message
		err      error
	}
	polled := make(chan pollResult, 1)
	go func() {
		var received []Message
		n, err := sub.Poll(ctx, func(_ context.Context, msg Message) error {
			received = append(received, msg)
			return nil
		})
		polled <- pollResult{n: n, messages: received, err: err}
	}()
	time.Sleep(100 * time.Millisecond)

	pub, err := DialPublication(PublicationConfig{
		StreamID:   241,
		SessionID:  341,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	sent := make(chan error, 1)
	go func() {
		sent <- pub.Send(ctx, []byte("external subscription payload"))
	}()

	var result pollResult
	select {
	case result = <-polled:
		if result.err != nil {
			t.Fatal(result.err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if result.n != 1 || len(result.messages) != 1 ||
		result.messages[0].StreamID != 241 ||
		result.messages[0].SessionID != 341 ||
		string(result.messages[0].Payload) != "external subscription payload" ||
		result.messages[0].Remote == nil {
		t.Fatalf("unexpected external poll result n=%d messages=%#v", result.n, result.messages)
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	images := sub.Images()
	if len(images) != 1 ||
		images[0].StreamID != 241 ||
		images[0].SessionID != 341 ||
		images[0].LastSequence != result.messages[0].Sequence ||
		images[0].CurrentPosition == 0 ||
		images[0].Source == "" {
		t.Fatalf("unexpected external images: %#v", images)
	}
	lagReports := sub.LagReports()
	if len(lagReports) != 1 ||
		lagReports[0].StreamID != images[0].StreamID ||
		lagReports[0].SessionID != images[0].SessionID ||
		lagReports[0].CurrentPosition != images[0].CurrentPosition {
		t.Fatalf("unexpected external lag reports: %#v images=%#v", lagReports, images)
	}
	if reports := sub.LossReports(); len(reports) != 0 {
		t.Fatalf("unexpected external loss reports: %#v", reports)
	}

	type controlledResult struct {
		n        int
		messages []Message
		err      error
	}
	controlled := make(chan controlledResult, 1)
	go func() {
		var messages []Message
		n, err := sub.ControlledPollN(ctx, 5, func(_ context.Context, msg Message) ControlledPollAction {
			messages = append(messages, msg)
			return ControlledPollBreak
		})
		controlled <- controlledResult{n: n, messages: messages, err: err}
	}()
	time.Sleep(100 * time.Millisecond)

	sentControlled := make(chan error, 2)
	go func() {
		sentControlled <- pub.Send(ctx, []byte("controlled one"))
		sentControlled <- pub.Send(ctx, []byte("controlled two"))
	}()

	var controlledPoll controlledResult
	select {
	case controlledPoll = <-controlled:
		if controlledPoll.err != nil {
			t.Fatal(controlledPoll.err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if controlledPoll.n != 1 || len(controlledPoll.messages) != 1 ||
		string(controlledPoll.messages[0].Payload) != "controlled one" {
		t.Fatalf("unexpected controlled poll result: %#v", controlledPoll)
	}

	var afterBreak []Message
	n, err := sub.Poll(ctx, func(_ context.Context, msg Message) error {
		afterBreak = append(afterBreak, msg)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || len(afterBreak) != 1 || string(afterBreak[0].Payload) != "controlled two" {
		t.Fatalf("controlled poll swallowed message after break n=%d messages=%#v", n, afterBreak)
	}
	for i := 0; i < 2; i++ {
		if err := <-sentControlled; err != nil {
			t.Fatal(err)
		}
	}

	sentBatch := make(chan error, 3)
	go func() {
		sentBatch <- pub.Send(ctx, []byte("polln one"))
		sentBatch <- pub.Send(ctx, []byte("polln two"))
		sentBatch <- pub.Send(ctx, []byte("polln three"))
	}()
	var batch []Message
	n, err = sub.PollN(ctx, 3, func(_ context.Context, msg Message) error {
		batch = append(batch, msg)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 ||
		len(batch) != 3 ||
		string(batch[0].Payload) != "polln one" ||
		string(batch[1].Payload) != "polln two" ||
		string(batch[2].Payload) != "polln three" {
		t.Fatalf("external PollN did not accumulate messages n=%d messages=%#v", n, batch)
	}
	for i := 0; i < 3; i++ {
		if err := <-sentBatch; err != nil {
			t.Fatal(err)
		}
	}

	abortSent := make(chan error, 1)
	go func() {
		abortSent <- pub.Send(ctx, []byte("controlled abort retained"))
	}()
	var abortMessages []Message
	n, err = sub.ControlledPoll(ctx, func(_ context.Context, msg Message) ControlledPollAction {
		abortMessages = append(abortMessages, msg)
		return ControlledPollAbort
	})
	if !errors.Is(err, ErrControlledPollAbort) || n != 0 ||
		len(abortMessages) != 1 ||
		string(abortMessages[0].Payload) != "controlled abort retained" {
		t.Fatalf("unexpected controlled abort n=%d err=%v messages=%#v", n, err, abortMessages)
	}
	if err := <-abortSent; err != nil {
		t.Fatal(err)
	}
	var afterAbort []Message
	n, err = sub.Poll(ctx, func(_ context.Context, msg Message) error {
		afterAbort = append(afterAbort, msg)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || len(afterAbort) != 1 || string(afterAbort[0].Payload) != "controlled abort retained" {
		t.Fatalf("controlled abort did not retain data ring message n=%d messages=%#v", n, afterAbort)
	}

	retrySent := make(chan error, 1)
	go func() {
		retrySent <- pub.Send(ctx, []byte("retry after handler error"))
	}()
	handlerErr := errors.New("handler failed before commit")
	n, err = sub.Poll(ctx, func(_ context.Context, msg Message) error {
		if string(msg.Payload) != "retry after handler error" {
			t.Fatalf("unexpected retry message before handler error: %#v", msg)
		}
		return handlerErr
	})
	if !errors.Is(err, handlerErr) || n != 0 {
		t.Fatalf("Poll() after handler error n=%d err=%v, want n=0 err=%v", n, err, handlerErr)
	}
	if err := <-retrySent; err != nil {
		t.Fatal(err)
	}
	var retried []Message
	n, err = sub.Poll(ctx, func(_ context.Context, msg Message) error {
		retried = append(retried, msg)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || len(retried) != 1 || string(retried[0].Payload) != "retry after handler error" {
		t.Fatalf("data ring did not retain message after handler error n=%d messages=%#v", n, retried)
	}

	ringSnapshot, ok, err := sub.DataRingSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !ok ||
		ringSnapshot.Path != sub.dataRingPath ||
		ringSnapshot.Capacity != 2*defaultDriverIPCSubscriptionDataRing ||
		ringSnapshot.Free == 0 {
		t.Fatalf("unexpected local data ring snapshot ok=%v snapshot=%#v", ok, ringSnapshot)
	}

	snapshot, err := client.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Subscriptions) != 1 || snapshot.Subscriptions[0].ID != sub.ID() ||
		snapshot.Subscriptions[0].ClientID != client.ID() {
		t.Fatalf("unexpected subscription snapshot: %#v", snapshot)
	}
	if snapshot.Subscriptions[0].DataRingPath != sub.dataRingPath ||
		!snapshot.Subscriptions[0].DataRingMapped ||
		snapshot.Subscriptions[0].DataRingCapacity != 2*defaultDriverIPCSubscriptionDataRing ||
		snapshot.Subscriptions[0].DataRingFree == 0 {
		t.Fatalf("unexpected external subscription data ring snapshot: %#v path=%q", snapshot.Subscriptions[0], sub.dataRingPath)
	}
	if err := sub.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(sub.dataRingPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("external subscription data ring remained after close: %v", err)
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

func TestExternalDriverProcessPumpsSubscriptionDataRing(t *testing.T) {
	root := t.TempDir()
	done := startExternalDriverProcessForTest(t, root)
	defer terminateExternalDriverProcessForTest(t, root, done)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, err := ConnectMediaDriver(ctx, DriverConnectionConfig{
		Directory:  root,
		ClientName: "external-pumped-subscriber",
		Timeout:    time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)

	sub, err := client.AddSubscription(ctx, SubscriptionConfig{
		StreamID:               248,
		LocalAddr:              "127.0.0.1:0",
		DriverDataRingCapacity: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close(ctx)

	pub, err := DialPublication(PublicationConfig{
		StreamID:   248,
		SessionID:  348,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	sent := make(chan error, 1)
	go func() {
		sent <- pub.Send(ctx, []byte("pumped payload"))
	}()
	waitForCondition(t, 5*time.Second, func() bool {
		snapshot, ok, err := sub.DataRingSnapshot()
		return err == nil && ok && snapshot.Used > 0
	})
	status, ok, err := sub.DataRingStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ok ||
		status.Path != sub.dataRingPath ||
		status.Capacity != 4096 ||
		status.Used == 0 ||
		status.LocalPendingMessages != 0 ||
		status.ServerPendingMessages != 0 {
		t.Fatalf("unexpected pumped data ring status ok=%v status=%#v", ok, status)
	}
	select {
	case err := <-sent:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	var received []Message
	n, err := sub.Poll(ctx, func(_ context.Context, msg Message) error {
		received = append(received, msg)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 ||
		len(received) != 1 ||
		received[0].StreamID != 248 ||
		received[0].SessionID != 348 ||
		string(received[0].Payload) != "pumped payload" {
		t.Fatalf("unexpected pumped poll result n=%d messages=%#v", n, received)
	}
	if got := sub.PendingMessages(); got != 0 {
		t.Fatalf("PendingMessages() = %d, want 0", got)
	}
	status, ok, err = sub.DataRingStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || status.Used != 0 || status.LocalPendingMessages != 0 || status.ServerPendingMessages != 0 {
		t.Fatalf("unexpected drained data ring status ok=%v status=%#v", ok, status)
	}
}

func TestExternalDriverProcessReportsServerFallbackPending(t *testing.T) {
	root := t.TempDir()
	done := startExternalDriverProcessForTest(t, root)
	defer terminateExternalDriverProcessForTest(t, root, done)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, err := ConnectMediaDriver(ctx, DriverConnectionConfig{
		Directory:  root,
		ClientName: "external-server-fallback-status",
		Timeout:    time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)

	sub, err := client.AddSubscription(ctx, SubscriptionConfig{
		StreamID:               247,
		LocalAddr:              "127.0.0.1:0",
		DriverDataRingCapacity: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close(ctx)

	pub, err := DialPublication(PublicationConfig{
		StreamID:   247,
		SessionID:  347,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	payload := bytes.Repeat([]byte("s"), 5000)
	sent := make(chan error, 1)
	go func() {
		sent <- pub.Send(ctx, payload)
	}()
	waitForCondition(t, 5*time.Second, func() bool {
		status, ok, err := sub.DataRingStatus(ctx)
		return err == nil && ok && status.ServerPendingMessages == 1
	})
	select {
	case err := <-sent:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	status, ok, err := sub.DataRingStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || status.ServerPendingMessages != 1 || status.LocalPendingMessages != 0 {
		t.Fatalf("unexpected server fallback status ok=%v status=%#v", ok, status)
	}

	handlerErr := errors.New("server fallback handler failed")
	n, err := sub.Poll(ctx, func(_ context.Context, msg Message) error {
		if !bytes.Equal(msg.Payload, payload) {
			t.Fatalf("unexpected server fallback payload: %#v", msg)
		}
		return handlerErr
	})
	if !errors.Is(err, handlerErr) || n != 0 {
		t.Fatalf("Poll() n=%d err=%v, want n=0 err=%v", n, err, handlerErr)
	}
	status, ok, err = sub.DataRingStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || status.ServerPendingMessages != 0 || status.LocalPendingMessages != 1 {
		t.Fatalf("unexpected local fallback status ok=%v status=%#v", ok, status)
	}

	var retried []Message
	n, err = sub.Poll(ctx, func(_ context.Context, msg Message) error {
		retried = append(retried, msg)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || len(retried) != 1 || !bytes.Equal(retried[0].Payload, payload) {
		t.Fatalf("fallback message was not retained after handler error n=%d messages=%#v", n, retried)
	}
	status, ok, err = sub.DataRingStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || status.ServerPendingMessages != 0 || status.LocalPendingMessages != 0 {
		t.Fatalf("unexpected cleared fallback status ok=%v status=%#v", ok, status)
	}
}

func TestExternalDriverFallbackMessagesRemainPendingAfterHandlerErrorAndAbort(t *testing.T) {
	root := t.TempDir()
	done := startExternalDriverProcessWithConfigForTest(t, DriverProcessConfig{
		Directory:               root,
		DisableSubscriptionPump: true,
	})
	defer terminateExternalDriverProcessForTest(t, root, done)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, err := ConnectMediaDriver(ctx, DriverConnectionConfig{
		Directory:  root,
		ClientName: "fallback-retention",
		Timeout:    time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)

	sub, err := client.AddSubscription(ctx, SubscriptionConfig{
		StreamID:               249,
		LocalAddr:              "127.0.0.1:0",
		DriverDataRingCapacity: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close(ctx)
	if got := sub.PendingMessages(); got != 0 {
		t.Fatalf("PendingMessages() = %d, want 0", got)
	}

	payload := bytes.Repeat([]byte("x"), 5000)
	type pollResult struct {
		n        int
		messages []Message
		err      error
	}
	polled := make(chan pollResult, 1)
	handlerErr := errors.New("fallback handler failed")
	go func() {
		var failed []Message
		n, err := sub.Poll(ctx, func(_ context.Context, msg Message) error {
			failed = append(failed, msg)
			return handlerErr
		})
		polled <- pollResult{n: n, messages: failed, err: err}
	}()
	time.Sleep(100 * time.Millisecond)

	pub, err := DialPublication(PublicationConfig{
		StreamID:   249,
		SessionID:  349,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	sent := make(chan error, 1)
	go func() {
		sent <- pub.Send(ctx, payload)
	}()
	result := <-polled
	if !errors.Is(result.err, handlerErr) || result.n != 0 ||
		len(result.messages) != 1 ||
		!bytes.Equal(result.messages[0].Payload, payload) {
		t.Fatalf("unexpected fallback handler error result=%#v", result)
	}
	if got := sub.PendingMessages(); got != 1 {
		t.Fatalf("PendingMessages() after handler error = %d, want 1", got)
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	var retried []Message
	n, err := sub.Poll(ctx, func(_ context.Context, msg Message) error {
		retried = append(retried, msg)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || len(retried) != 1 || !bytes.Equal(retried[0].Payload, payload) {
		t.Fatalf("fallback message was not retained after handler error n=%d messages=%#v", n, retried)
	}
	if got := sub.PendingMessages(); got != 0 {
		t.Fatalf("PendingMessages() after retry = %d, want 0", got)
	}

	abortPayload := bytes.Repeat([]byte("y"), 5000)
	type controlledResult struct {
		n        int
		messages []Message
		err      error
	}
	controlled := make(chan controlledResult, 1)
	go func() {
		var aborted []Message
		n, err := sub.ControlledPoll(ctx, func(_ context.Context, msg Message) ControlledPollAction {
			aborted = append(aborted, msg)
			return ControlledPollAbort
		})
		controlled <- controlledResult{n: n, messages: aborted, err: err}
	}()
	time.Sleep(100 * time.Millisecond)

	abortSent := make(chan error, 1)
	go func() {
		abortSent <- pub.Send(ctx, abortPayload)
	}()
	controlledPoll := <-controlled
	if !errors.Is(controlledPoll.err, ErrControlledPollAbort) || controlledPoll.n != 0 ||
		len(controlledPoll.messages) != 1 ||
		!bytes.Equal(controlledPoll.messages[0].Payload, abortPayload) {
		t.Fatalf("unexpected fallback controlled abort result=%#v", controlledPoll)
	}
	if got := sub.PendingMessages(); got != 1 {
		t.Fatalf("PendingMessages() after controlled abort = %d, want 1", got)
	}
	if err := <-abortSent; err != nil {
		t.Fatal(err)
	}
	var afterAbort []Message
	n, err = sub.Poll(ctx, func(_ context.Context, msg Message) error {
		afterAbort = append(afterAbort, msg)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || len(afterAbort) != 1 || !bytes.Equal(afterAbort[0].Payload, abortPayload) {
		t.Fatalf("fallback message was not retained after controlled abort n=%d messages=%#v", n, afterAbort)
	}
	if got := sub.PendingMessages(); got != 0 {
		t.Fatalf("PendingMessages() after controlled retry = %d, want 0", got)
	}
}

func TestConnectMediaDriverRejectsNegativeTimeout(t *testing.T) {
	if _, err := ConnectMediaDriver(context.Background(), DriverConnectionConfig{Timeout: -time.Second}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("ConnectMediaDriver() err = %v, want %v", err, ErrInvalidConfig)
	}
	if _, err := ConnectMediaDriver(context.Background(), DriverConnectionConfig{HeartbeatInterval: -time.Second}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("ConnectMediaDriver() err = %v, want %v", err, ErrInvalidConfig)
	}
}

func TestExternalDriverClientAutoHeartbeatKeepsResourcesActive(t *testing.T) {
	root := t.TempDir()
	done := startExternalDriverProcessWithConfigForTest(t, DriverProcessConfig{
		Directory: root,
		Driver: DriverConfig{
			ClientTimeout:   500 * time.Millisecond,
			CleanupInterval: 50 * time.Millisecond,
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := ConnectMediaDriver(ctx, DriverConnectionConfig{
		Directory:         root,
		ClientName:        "auto-heartbeat",
		Timeout:           time.Second,
		HeartbeatInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.AddSubscription(ctx, SubscriptionConfig{
		StreamID:  242,
		LocalAddr: "127.0.0.1:0",
	}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(1200 * time.Millisecond)
	snapshot, err := client.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Clients) != 1 ||
		len(snapshot.Subscriptions) != 1 ||
		snapshot.Counters.StaleClientsClosed != 0 {
		t.Fatalf("auto heartbeat did not keep external resources active: %#v", snapshot)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}

	terminateExternalDriverProcessForTest(t, root, done)
}

func TestExternalDriverStaleClientRemovesSubscriptionDataRing(t *testing.T) {
	root := t.TempDir()
	done := startExternalDriverProcessWithConfigForTest(t, DriverProcessConfig{
		Directory:           root,
		HeartbeatInterval:   20 * time.Millisecond,
		CommandRingCapacity: 64 * 1024,
		EventRingCapacity:   64 * 1024,
		Driver: DriverConfig{
			ClientTimeout:   120 * time.Millisecond,
			CleanupInterval: 20 * time.Millisecond,
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := ConnectMediaDriver(ctx, DriverConnectionConfig{
		Directory:            root,
		ClientName:           "stale-data-ring",
		Timeout:              time.Second,
		DisableAutoHeartbeat: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := client.AddSubscription(ctx, SubscriptionConfig{
		StreamID:  243,
		LocalAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	dataRingPath := sub.dataRingPath
	if dataRingPath == "" {
		t.Fatal("external subscription data ring path is empty")
	}
	waitForPath(t, dataRingPath)
	waitForCondition(t, 5*time.Second, func() bool {
		_, err := os.Stat(dataRingPath)
		return errors.Is(err, os.ErrNotExist)
	})
	if err := client.Close(ctx); err != nil && !errors.Is(err, ErrDriverClientClosed) {
		t.Fatal(err)
	}

	terminateExternalDriverProcessForTest(t, root, done)
}

func TestExternalDriverClientsUseDedicatedResponseRings(t *testing.T) {
	root := t.TempDir()
	done := startExternalDriverProcessForTest(t, root)
	defer terminateExternalDriverProcessForTest(t, root, done)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	clientOne, err := ConnectMediaDriver(ctx, DriverConnectionConfig{
		Directory:  root,
		ClientName: "response-ring-one",
		Timeout:    time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clientOne.Close(ctx)
	clientTwo, err := ConnectMediaDriver(ctx, DriverConnectionConfig{
		Directory:  root,
		ClientName: "response-ring-two",
		Timeout:    time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clientTwo.Close(ctx)

	if clientOne.responseRingPath == "" ||
		clientTwo.responseRingPath == "" ||
		clientOne.responseRingPath == clientTwo.responseRingPath {
		t.Fatalf("clients did not get distinct response rings: one=%q two=%q", clientOne.responseRingPath, clientTwo.responseRingPath)
	}
	if _, err := os.Stat(clientOne.responseRingPath); err != nil {
		t.Fatalf("stat client one response ring: %v", err)
	}
	if _, err := os.Stat(clientTwo.responseRingPath); err != nil {
		t.Fatalf("stat client two response ring: %v", err)
	}

	errs := make(chan error, 2)
	for _, client := range []*DriverClient{clientOne, clientTwo} {
		client := client
		go func() {
			_, err := client.Snapshot(ctx)
			errs <- err
		}()
	}
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

func startExternalDriverProcessForTest(t *testing.T, root string) <-chan error {
	t.Helper()
	return startExternalDriverProcessWithConfigForTest(t, DriverProcessConfig{Directory: root})
}

func startExternalDriverProcessWithConfigForTest(t *testing.T, cfg DriverProcessConfig) <-chan error {
	t.Helper()
	if cfg.CommandRingCapacity == 0 {
		cfg.CommandRingCapacity = 64 * 1024
	}
	if cfg.EventRingCapacity == 0 {
		cfg.EventRingCapacity = 64 * 1024
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 20 * time.Millisecond
	}
	if cfg.IdleStrategy == nil {
		cfg.IdleStrategy = SleepingIdleStrategy{Duration: time.Millisecond}
	}
	cfg.ResetIPC = true
	done := make(chan error, 1)
	go func() {
		done <- RunMediaDriverProcess(context.Background(), cfg)
	}()
	layout := waitForDriverProcessStatus(t, cfg.Directory, func(status DriverProcessStatus) bool {
		return status.Active && !status.Stale
	}).Layout
	waitForPath(t, layout.CommandRingFile)
	waitForPath(t, layout.EventRingFile)
	waitForDriverIPCReady(t, cfg.Directory)
	select {
	case err := <-done:
		t.Fatalf("driver process exited early: %v", err)
	default:
	}
	return done
}

func waitForDriverIPCReady(t *testing.T, root string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ipc, err := OpenDriverIPC(DriverIPCConfig{Directory: root})
		if err == nil {
			_ = ipc.Close()
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("driver IPC was not ready in %s", root)
}

func waitForCondition(t *testing.T, timeout time.Duration, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if done() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition was not met within %s", timeout)
}

func terminateExternalDriverProcessForTest(t *testing.T, root string, done <-chan error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
