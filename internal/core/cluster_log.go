package core

import (
	"context"
	"sync"
	"time"
)

type ClusterLogEntryType string

const (
	ClusterLogEntryIngress        ClusterLogEntryType = "ingress"
	ClusterLogEntryTimerSchedule  ClusterLogEntryType = "timer_schedule"
	ClusterLogEntryTimerCancel    ClusterLogEntryType = "timer_cancel"
	ClusterLogEntryTimerFire      ClusterLogEntryType = "timer_fire"
	ClusterLogEntryServiceMessage ClusterLogEntryType = "service_message"
)

type ClusterLogEntry struct {
	Position      int64                `json:"position"`
	Type          ClusterLogEntryType  `json:"type"`
	SessionID     ClusterSessionID     `json:"session_id"`
	CorrelationID ClusterCorrelationID `json:"correlation_id"`
	TimerID       ClusterTimerID       `json:"timer_id,omitempty"`
	Deadline      time.Time            `json:"deadline,omitempty"`
	SourceService string               `json:"source_service,omitempty"`
	TargetService string               `json:"target_service,omitempty"`
	Payload       []byte               `json:"payload"`
}

type ClusterLog interface {
	Append(context.Context, ClusterLogEntry) (ClusterLogEntry, error)
	Snapshot(context.Context) ([]ClusterLogEntry, error)
	LastPosition(context.Context) (int64, error)
	Close() error
}

type InMemoryClusterLog struct {
	mu      sync.Mutex
	entries []ClusterLogEntry
	closed  bool
}

func NewInMemoryClusterLog() *InMemoryClusterLog {
	return &InMemoryClusterLog{}
}

func (l *InMemoryClusterLog) Append(ctx context.Context, entry ClusterLogEntry) (ClusterLogEntry, error) {
	if l == nil {
		return ClusterLogEntry{}, ErrClusterLogClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ClusterLogEntry{}, ctx.Err()
	default:
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ClusterLogEntry{}, ErrClusterLogClosed
	}
	nextPosition := int64(1)
	if len(l.entries) > 0 {
		nextPosition = l.entries[len(l.entries)-1].Position + 1
	}
	if entry.Position == 0 {
		entry.Position = nextPosition
	} else if len(l.entries) > 0 && entry.Position != nextPosition {
		return ClusterLogEntry{}, ErrClusterLogPosition
	} else if entry.Position < 1 {
		return ClusterLogEntry{}, ErrClusterLogPosition
	}
	entry.Payload = cloneBytes(entry.Payload)
	l.entries = append(l.entries, entry)
	return cloneClusterLogEntry(entry), nil
}

func (l *InMemoryClusterLog) Snapshot(ctx context.Context) ([]ClusterLogEntry, error) {
	if l == nil {
		return nil, ErrClusterLogClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil, ErrClusterLogClosed
	}
	entries := make([]ClusterLogEntry, len(l.entries))
	for i, entry := range l.entries {
		entries[i] = cloneClusterLogEntry(entry)
	}
	return entries, nil
}

func (l *InMemoryClusterLog) LastPosition(ctx context.Context) (int64, error) {
	if l == nil {
		return 0, ErrClusterLogClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return 0, ErrClusterLogClosed
	}
	if len(l.entries) == 0 {
		return 0, nil
	}
	return l.entries[len(l.entries)-1].Position, nil
}

func (l *InMemoryClusterLog) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	l.closed = true
	l.entries = nil
	return nil
}

func cloneClusterLogEntry(entry ClusterLogEntry) ClusterLogEntry {
	entry.Payload = cloneBytes(entry.Payload)
	return entry
}

func cloneClusterLogEntries(entries []ClusterLogEntry) []ClusterLogEntry {
	if len(entries) == 0 {
		return nil
	}
	cloned := make([]ClusterLogEntry, len(entries))
	for i, entry := range entries {
		cloned[i] = cloneClusterLogEntry(entry)
	}
	return cloned
}
