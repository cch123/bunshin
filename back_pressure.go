package bunshin

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var ErrBackPressure = errors.New("bunshin: back pressure")

type publicationWindow struct {
	mu     sync.Mutex
	limit  int
	used   int
	notify chan struct{}
}

func newPublicationWindow(limit int) (*publicationWindow, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("invalid publication window bytes: %d", limit)
	}
	return &publicationWindow{
		limit:  limit,
		notify: make(chan struct{}),
	}, nil
}

func (w *publicationWindow) setLimit(limit int) error {
	if limit <= 0 {
		return fmt.Errorf("invalid publication window bytes: %d", limit)
	}
	w.mu.Lock()
	w.limit = limit
	close(w.notify)
	w.notify = make(chan struct{})
	w.mu.Unlock()
	return nil
}

func (w *publicationWindow) reserve(ctx context.Context, bytes int, closed <-chan struct{}) (bool, error) {
	if bytes <= 0 {
		return false, fmt.Errorf("invalid publication window reservation bytes: %d", bytes)
	}
	if bytes > w.limit {
		return true, fmt.Errorf("%w: frame requires %d bytes, window has %d", ErrBackPressure, bytes, w.limit)
	}

	var backPressured bool
	for {
		w.mu.Lock()
		if w.used+bytes <= w.limit {
			w.used += bytes
			w.mu.Unlock()
			return backPressured, nil
		}
		notify := w.notify
		w.mu.Unlock()

		backPressured = true
		select {
		case <-notify:
		case <-closed:
			return backPressured, ErrClosed
		case <-ctx.Done():
			return backPressured, fmt.Errorf("%w: %w", ErrBackPressure, ctx.Err())
		}
	}
}

func (w *publicationWindow) tryReserve(bytes int, closed <-chan struct{}) error {
	if bytes <= 0 {
		return fmt.Errorf("invalid publication window reservation bytes: %d", bytes)
	}
	select {
	case <-closed:
		return ErrClosed
	default:
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if bytes > w.limit {
		return fmt.Errorf("%w: frame requires %d bytes, window has %d", ErrBackPressure, bytes, w.limit)
	}
	if w.used+bytes > w.limit {
		return ErrBackPressure
	}
	w.used += bytes
	return nil
}

func (w *publicationWindow) release(bytes int) {
	w.mu.Lock()
	w.used -= bytes
	if w.used < 0 {
		w.used = 0
	}
	close(w.notify)
	w.notify = make(chan struct{})
	w.mu.Unlock()
}
