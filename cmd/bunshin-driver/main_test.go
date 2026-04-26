package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/xargin/bunshin"
)

func TestRingsDriverReportsSubscriptionDataRings(t *testing.T) {
	root := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- bunshin.RunMediaDriverProcess(ctx, bunshin.DriverProcessConfig{
			Directory:           root,
			CommandRingCapacity: 64 * 1024,
			EventRingCapacity:   64 * 1024,
			ResetIPC:            true,
			HeartbeatInterval:   10 * time.Millisecond,
			IdleStrategy:        bunshin.SleepingIdleStrategy{Duration: time.Millisecond},
		})
	}()
	waitForDriverReady(t, root)
	select {
	case err := <-done:
		t.Fatalf("driver process exited early: %v", err)
	default:
	}

	clientCtx, clientCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer clientCancel()
	client, err := bunshin.ConnectMediaDriver(clientCtx, bunshin.DriverConnectionConfig{
		Directory:  root,
		ClientName: "rings-tool-test",
		Timeout:    5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := client.AddSubscription(clientCtx, bunshin.SubscriptionConfig{
		StreamID:               77,
		LocalAddr:              "127.0.0.1:0",
		DriverDataRingCapacity: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}

	report, err := ringsDriver([]string{"-dir", root, "-timeout", "1s"})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Subscriptions) != 1 ||
		report.Subscriptions[0].ResourceID != sub.ID() ||
		report.Subscriptions[0].StreamID != 77 ||
		report.Subscriptions[0].Capacity != 4096 ||
		report.Subscriptions[0].Free == 0 ||
		report.Subscriptions[0].PendingMessages != 0 ||
		report.Subscriptions[0].Path == "" {
		t.Fatalf("unexpected rings report: %#v", report)
	}
	pub, err := bunshin.DialPublication(bunshin.PublicationConfig{
		StreamID:   77,
		SessionID:  177,
		RemoteAddr: sub.LocalAddr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	payload := bytes.Repeat([]byte("r"), 5000)
	sent := make(chan error, 1)
	go func() {
		sent <- pub.Send(clientCtx, payload)
	}()
	report = waitForRingsPending(t, root, sub.ID(), 1)
	if got := report.Subscriptions[0].PendingMessages; got != 1 {
		t.Fatalf("rings report pending messages = %d, want 1: %#v", got, report)
	}
	select {
	case err := <-sent:
		if err != nil {
			t.Fatal(err)
		}
	case <-clientCtx.Done():
		t.Fatal(clientCtx.Err())
	}
	flushed, err := flushDriver([]string{"-dir", root, "-timeout", "1s"})
	if err != nil {
		t.Fatal(err)
	}
	if len(flushed.Rings.Subscriptions) != 1 ||
		flushed.Rings.Subscriptions[0].ResourceID != sub.ID() ||
		flushed.Rings.Subscriptions[0].Capacity != 4096 {
		t.Fatalf("unexpected flushed rings report: %#v", flushed.Rings)
	}
	if flushed.Rings.Subscriptions[0].PendingMessages != 1 {
		t.Fatalf("unexpected flushed pending messages: %#v", flushed.Rings)
	}
	offline, err := ringsDriver([]string{"-dir", root, "-report"})
	if err != nil {
		t.Fatal(err)
	}
	if len(offline.Subscriptions) != 1 ||
		offline.Subscriptions[0].ResourceID != sub.ID() ||
		offline.Subscriptions[0].Capacity != 4096 {
		t.Fatalf("unexpected offline rings report: %#v", offline)
	}
	if offline.Subscriptions[0].PendingMessages != 1 {
		t.Fatalf("unexpected offline pending messages: %#v", offline)
	}
	if err := sub.Close(clientCtx); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(clientCtx); err != nil {
		t.Fatal(err)
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("RunMediaDriverProcess() err = %v, want %v", err, context.Canceled)
	}
}

func waitForRingsPending(t *testing.T, root string, resourceID bunshin.DriverResourceID, want int) bunshin.DriverRingsReportFile {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var last bunshin.DriverRingsReportFile
	var lastErr error
	for time.Now().Before(deadline) {
		report, err := ringsDriver([]string{"-dir", root, "-timeout", "1s"})
		if err != nil {
			lastErr = err
			time.Sleep(5 * time.Millisecond)
			continue
		}
		last = report
		for _, subscription := range report.Subscriptions {
			if subscription.ResourceID == resourceID && subscription.PendingMessages == want {
				return report
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("rings pending messages did not become %d: report=%#v err=%v", want, last, lastErr)
	return bunshin.DriverRingsReportFile{}
}

func waitForDriverReady(t *testing.T, root string) {
	t.Helper()
	layout, err := bunshin.ResolveDriverDirectoryLayout(root)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		status, err := bunshin.CheckDriverProcess(root, time.Second)
		if err == nil && status.Active && !status.Stale {
			if _, err := os.Stat(layout.CommandRingFile); err != nil {
				time.Sleep(5 * time.Millisecond)
				continue
			}
			if _, err := os.Stat(layout.EventRingFile); err != nil {
				time.Sleep(5 * time.Millisecond)
				continue
			}
			ipc, err := bunshin.OpenDriverIPC(bunshin.DriverIPCConfig{Directory: root})
			if err == nil {
				_ = ipc.Close()
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("driver process was not ready in %s", root)
}
