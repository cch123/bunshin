package core

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPublicationWindowBackPressureAndRelease(t *testing.T) {
	window, err := newPublicationWindow(64)
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})

	backPressured, err := window.reserve(context.Background(), 64, closed)
	if err != nil {
		t.Fatal(err)
	}
	if backPressured {
		t.Fatal("first reservation reported back pressure")
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	backPressured, err = window.reserve(waitCtx, 64, closed)
	if !backPressured {
		t.Fatal("second reservation did not report back pressure")
	}
	if !errors.Is(err, ErrBackPressure) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second reservation err = %v", err)
	}

	window.release(64)
	backPressured, err = window.reserve(context.Background(), 64, closed)
	if err != nil {
		t.Fatal(err)
	}
	if backPressured {
		t.Fatal("reservation after release reported back pressure")
	}
}

func TestPublicationWindowRejectsUnfitFrame(t *testing.T) {
	window, err := newPublicationWindow(64)
	if err != nil {
		t.Fatal(err)
	}
	backPressured, err := window.reserve(context.Background(), 96, make(chan struct{}))
	if !backPressured {
		t.Fatal("oversized reservation did not report back pressure")
	}
	if !errors.Is(err, ErrBackPressure) {
		t.Fatalf("reservation err = %v, want %v", err, ErrBackPressure)
	}
}
