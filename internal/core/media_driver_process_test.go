package core

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func TestDriverProcessHeartbeatAndStatus(t *testing.T) {
	root := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RunMediaDriverProcess(ctx, DriverProcessConfig{
			Directory:         root,
			ResetIPC:          true,
			HeartbeatInterval: 10 * time.Millisecond,
			IdleStrategy:      SleepingIdleStrategy{Duration: time.Millisecond},
		})
	}()

	var first DriverProcessStatus
	waitForDriverProcessStatus(t, root, func(status DriverProcessStatus) bool {
		first = status
		return status.Active && !status.Stale
	})
	waitForDriverProcessStatus(t, root, func(status DriverProcessStatus) bool {
		return status.Mark.UpdatedAt.After(first.Mark.UpdatedAt)
	})

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("RunMediaDriverProcess() err = %v, want %v", err, context.Canceled)
	}
	status, err := CheckDriverProcess(root, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if status.Active || status.Mark.Status != DriverDirectoryStatusClosed {
		t.Fatalf("unexpected closed process status: %#v", status)
	}
}

func TestDriverProcessTerminateViaIPC(t *testing.T) {
	root := t.TempDir()
	done := make(chan error, 1)
	go func() {
		done <- RunMediaDriverProcess(context.Background(), DriverProcessConfig{
			Directory:           root,
			ResetIPC:            true,
			CommandRingCapacity: 4096,
			EventRingCapacity:   4096,
			HeartbeatInterval:   10 * time.Millisecond,
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
		t.Fatalf("driver process exited before terminate command: %v", err)
	default:
	}

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
	status, err := CheckDriverProcess(root, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if status.Active || status.Mark.Status != DriverDirectoryStatusClosed {
		t.Fatalf("unexpected terminated process status: %#v", status)
	}
}

func TestDriverProcessRestartsAfterCleanShutdown(t *testing.T) {
	root := t.TempDir()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RunMediaDriverProcess(ctx, DriverProcessConfig{
			Directory:         root,
			ResetIPC:          true,
			HeartbeatInterval: 10 * time.Millisecond,
			IdleStrategy:      SleepingIdleStrategy{Duration: time.Millisecond},
		})
	}()
	waitForDriverProcessStatus(t, root, func(status DriverProcessStatus) bool {
		return status.Active && !status.Stale
	})
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("first RunMediaDriverProcess() err = %v, want %v", err, context.Canceled)
	}

	restartCtx, restartCancel := context.WithCancel(context.Background())
	restarted := make(chan error, 1)
	go func() {
		restarted <- RunMediaDriverProcess(restartCtx, DriverProcessConfig{
			Directory:         root,
			ResetIPC:          true,
			HeartbeatInterval: 10 * time.Millisecond,
			IdleStrategy:      SleepingIdleStrategy{Duration: time.Millisecond},
		})
	}()
	status := waitForDriverProcessStatus(t, root, func(status DriverProcessStatus) bool {
		return status.Active && !status.Stale
	})
	if status.Mark.ClosedAt != nil {
		t.Fatalf("restarted driver mark still has closed_at: %#v", status.Mark)
	}
	restartCancel()
	if err := <-restarted; !errors.Is(err, context.Canceled) {
		t.Fatalf("second RunMediaDriverProcess() err = %v, want %v", err, context.Canceled)
	}
}

func TestCheckDriverProcessDetectsStaleMark(t *testing.T) {
	root := t.TempDir()
	layout, err := ResolveDriverDirectoryLayout(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().Add(-time.Minute)
	if err := writeDriverJSONFile(layout.MarkFile, DriverMarkFile{
		Version:   driverDirectoryLayoutVersion,
		PID:       os.Getpid(),
		Status:    DriverDirectoryStatusActive,
		StartedAt: old,
		UpdatedAt: old,
	}); err != nil {
		t.Fatal(err)
	}
	status, err := CheckDriverProcess(root, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Active || !status.Stale || status.HeartbeatAge < time.Second {
		t.Fatalf("unexpected stale status: %#v", status)
	}
}

func TestStartMediaDriverRejectsActiveDirectory(t *testing.T) {
	root := t.TempDir()
	layout, err := ResolveDriverDirectoryLayout(root)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := writeDriverJSONFile(layout.MarkFile, DriverMarkFile{
		Version:   driverDirectoryLayoutVersion,
		PID:       os.Getpid(),
		Status:    DriverDirectoryStatusActive,
		StartedAt: now,
		UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	driver, err := StartMediaDriver(DriverConfig{
		Directory:             root,
		DirectoryStaleTimeout: time.Hour,
	})
	if err == nil {
		_ = driver.Close()
		t.Fatal("StartMediaDriver() err = nil, want active directory error")
	}
	if !errors.Is(err, ErrDriverDirectoryActive) {
		t.Fatalf("StartMediaDriver() err = %v, want %v", err, ErrDriverDirectoryActive)
	}
}

func TestStartMediaDriverRecoversStaleActiveDirectory(t *testing.T) {
	root := t.TempDir()
	layout, err := ResolveDriverDirectoryLayout(root)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().Add(-time.Hour)
	if err := writeDriverJSONFile(layout.MarkFile, DriverMarkFile{
		Version:   driverDirectoryLayoutVersion,
		PID:       os.Getpid(),
		Status:    DriverDirectoryStatusActive,
		StartedAt: old,
		UpdatedAt: old,
	}); err != nil {
		t.Fatal(err)
	}

	driver, err := StartMediaDriver(DriverConfig{
		Directory:             root,
		DirectoryStaleTimeout: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()

	mark := readDriverJSON[DriverMarkFile](t, layout.MarkFile)
	if mark.Status != DriverDirectoryStatusActive || !mark.StartedAt.After(old) || mark.ClosedAt != nil {
		t.Fatalf("unexpected recovered mark file: %#v", mark)
	}
}

func waitForDriverProcessStatus(t *testing.T, root string, done func(DriverProcessStatus) bool) DriverProcessStatus {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	var last DriverProcessStatus
	var lastErr error
	for time.Now().Before(deadline) {
		status, err := CheckDriverProcess(root, 100*time.Millisecond)
		if err == nil {
			last = status
			if done(status) {
				return status
			}
		} else {
			lastErr = err
		}
		time.Sleep(5 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("timed out waiting for driver process status: last err=%v", lastErr)
	}
	t.Fatalf("timed out waiting for driver process status: %#v", last)
	return DriverProcessStatus{}
}

func waitForPath(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		} else {
			lastErr = err
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s: %v", path, lastErr)
}
