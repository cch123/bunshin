//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package bunshin

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestIPCRingOfferPoll(t *testing.T) {
	ring, err := OpenIPCRing(IPCRingConfig{
		Path:     filepath.Join(t.TempDir(), "ipc.ring"),
		Capacity: 256,
		Reset:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ring.Close()

	if err := ring.Offer([]byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := ring.Offer([]byte("two")); err != nil {
		t.Fatal(err)
	}

	var got []string
	n, err := ring.PollN(2, func(payload []byte) error {
		got = append(got, string(payload))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("PollN() n = %d, want 2", n)
	}
	if len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Fatalf("unexpected payloads: %#v", got)
	}

	if _, err := ring.Poll(func([]byte) error { return nil }); !errors.Is(err, ErrIPCRingEmpty) {
		t.Fatalf("Poll() err = %v, want %v", err, ErrIPCRingEmpty)
	}
	snapshot, err := ring.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Used != 0 || snapshot.Free != 256 || snapshot.ReadPosition != snapshot.WritePosition {
		t.Fatalf("unexpected snapshot: %#v", snapshot)
	}
}

func TestIPCRingWrapsRecords(t *testing.T) {
	ring, err := OpenIPCRing(IPCRingConfig{
		Path:     filepath.Join(t.TempDir(), "ipc.ring"),
		Capacity: 128,
		Reset:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ring.Close()

	first := bytes.Repeat([]byte("a"), 50)
	second := bytes.Repeat([]byte("b"), 50)
	third := bytes.Repeat([]byte("c"), 50)
	if err := ring.Offer(first); err != nil {
		t.Fatal(err)
	}
	if err := ring.Offer(second); err != nil {
		t.Fatal(err)
	}

	var got [][]byte
	if _, err := ring.Poll(func(payload []byte) error {
		got = append(got, payload)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := ring.Offer(third); err != nil {
		t.Fatal(err)
	}
	n, err := ring.PollN(2, func(payload []byte) error {
		got = append(got, payload)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("PollN() n = %d, want 2", n)
	}
	if len(got) != 3 || !bytes.Equal(got[0], first) || !bytes.Equal(got[1], second) || !bytes.Equal(got[2], third) {
		t.Fatalf("unexpected wrapped payloads: %#v", got)
	}
}

func TestIPCRingFullAndTooLarge(t *testing.T) {
	ring, err := OpenIPCRing(IPCRingConfig{
		Path:     filepath.Join(t.TempDir(), "ipc.ring"),
		Capacity: 64,
		Reset:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ring.Close()

	if err := ring.Offer(bytes.Repeat([]byte("x"), 61)); !errors.Is(err, ErrIPCRingMessageTooLarge) {
		t.Fatalf("Offer() err = %v, want %v", err, ErrIPCRingMessageTooLarge)
	}
	if err := ring.Offer(bytes.Repeat([]byte("a"), 28)); err != nil {
		t.Fatal(err)
	}
	if err := ring.Offer(bytes.Repeat([]byte("b"), 28)); err != nil {
		t.Fatal(err)
	}
	if err := ring.Offer([]byte("full")); !errors.Is(err, ErrIPCRingFull) {
		t.Fatalf("Offer() err = %v, want %v", err, ErrIPCRingFull)
	}
}

func TestIPCRingSharedMapping(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ipc.ring")
	writer, err := OpenIPCRing(IPCRingConfig{
		Path:     path,
		Capacity: 256,
		Reset:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	reader, err := OpenIPCRing(IPCRingConfig{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if err := writer.Offer([]byte("shared")); err != nil {
		t.Fatal(err)
	}
	n, err := reader.Poll(func(payload []byte) error {
		if string(payload) != "shared" {
			t.Fatalf("payload = %q, want shared", payload)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("Poll() n = %d, want 1", n)
	}

	snapshot, err := writer.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Used != 0 || snapshot.ReadPosition != snapshot.WritePosition {
		t.Fatalf("writer did not observe reader progress: %#v", snapshot)
	}
}

func TestIPCRingRejectsAfterClose(t *testing.T) {
	ring, err := OpenIPCRing(IPCRingConfig{
		Path:     filepath.Join(t.TempDir(), "ipc.ring"),
		Capacity: 256,
		Reset:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ring.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ring.Offer([]byte("closed")); !errors.Is(err, ErrIPCRingClosed) {
		t.Fatalf("Offer() err = %v, want %v", err, ErrIPCRingClosed)
	}
	if _, err := ring.Poll(func([]byte) error { return nil }); !errors.Is(err, ErrIPCRingClosed) {
		t.Fatalf("Poll() err = %v, want %v", err, ErrIPCRingClosed)
	}
	if _, err := ring.Snapshot(); !errors.Is(err, ErrIPCRingClosed) {
		t.Fatalf("Snapshot() err = %v, want %v", err, ErrIPCRingClosed)
	}
	if err := ring.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestIPCRingOfferContextWaitsForCapacity(t *testing.T) {
	ring, err := OpenIPCRing(IPCRingConfig{
		Path:     filepath.Join(t.TempDir(), "ipc.ring"),
		Capacity: 64,
		Reset:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ring.Close()

	if err := ring.Offer(bytes.Repeat([]byte("a"), 28)); err != nil {
		t.Fatal(err)
	}
	if err := ring.Offer(bytes.Repeat([]byte("b"), 28)); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- ring.OfferContext(ctx, []byte("c"), SleepingIdleStrategy{Duration: time.Millisecond})
	}()

	time.Sleep(10 * time.Millisecond)
	if _, err := ring.Poll(func([]byte) error { return nil }); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	var got [][]byte
	n, err := ring.PollN(2, func(payload []byte) error {
		got = append(got, payload)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 || len(got) != 2 || got[0][0] != 'b' || string(got[1]) != "c" {
		t.Fatalf("unexpected payloads after wait: n=%d got=%q", n, got)
	}
}

func TestIPCRingOfferContextDeadline(t *testing.T) {
	ring, err := OpenIPCRing(IPCRingConfig{
		Path:     filepath.Join(t.TempDir(), "ipc.ring"),
		Capacity: 64,
		Reset:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ring.Close()

	if err := ring.Offer(bytes.Repeat([]byte("a"), 28)); err != nil {
		t.Fatal(err)
	}
	if err := ring.Offer(bytes.Repeat([]byte("b"), 28)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	err = ring.OfferContext(ctx, []byte("c"), NoOpIdleStrategy{})
	if !errors.Is(err, ErrIPCRingFull) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("OfferContext() err = %v, want full deadline", err)
	}
}

func TestIPCRingPollContextWaitsForPayload(t *testing.T) {
	ring, err := OpenIPCRing(IPCRingConfig{
		Path:     filepath.Join(t.TempDir(), "ipc.ring"),
		Capacity: 256,
		Reset:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ring.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		time.Sleep(10 * time.Millisecond)
		_ = ring.Offer([]byte("payload"))
	}()

	var got string
	n, err := ring.PollContext(ctx, func(payload []byte) error {
		got = string(payload)
		return nil
	}, SleepingIdleStrategy{Duration: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || got != "payload" {
		t.Fatalf("PollContext() = (%d, %q), want payload", n, got)
	}
}

func TestIPCRingPollContextDeadline(t *testing.T) {
	ring, err := OpenIPCRing(IPCRingConfig{
		Path:     filepath.Join(t.TempDir(), "ipc.ring"),
		Capacity: 256,
		Reset:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ring.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, err = ring.PollContext(ctx, func([]byte) error { return nil }, NoOpIdleStrategy{})
	if !errors.Is(err, ErrIPCRingEmpty) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("PollContext() err = %v, want empty deadline", err)
	}
}
