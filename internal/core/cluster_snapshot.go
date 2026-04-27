package core

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrClusterSnapshotClosed           = errors.New("bunshin cluster snapshot: closed")
	ErrClusterSnapshotStoreUnavailable = errors.New("bunshin cluster snapshot: store unavailable")
	ErrClusterSnapshotUnsupported      = errors.New("bunshin cluster snapshot: service unsupported")
)

type ClusterSnapshotStore interface {
	Save(context.Context, ClusterStateSnapshot) error
	Load(context.Context) (ClusterStateSnapshot, bool, error)
	Close() error
}

type ClusterSnapshotService interface {
	SnapshotClusterState(context.Context, ClusterServiceContext) ([]byte, error)
	LoadClusterSnapshot(context.Context, ClusterStateSnapshot) error
}

type ClusterStateSnapshot struct {
	NodeID   ClusterNodeID  `json:"node_id"`
	Role     ClusterRole    `json:"role"`
	Position int64          `json:"position"`
	TakenAt  time.Time      `json:"taken_at"`
	Payload  []byte         `json:"payload"`
	Timers   []ClusterTimer `json:"timers,omitempty"`
}

type InMemoryClusterSnapshotStore struct {
	mu       sync.Mutex
	snapshot ClusterStateSnapshot
	has      bool
	closed   bool
}

func NewInMemoryClusterSnapshotStore() *InMemoryClusterSnapshotStore {
	return &InMemoryClusterSnapshotStore{}
}

func (s *InMemoryClusterSnapshotStore) Save(ctx context.Context, snapshot ClusterStateSnapshot) error {
	if s == nil {
		return ErrClusterSnapshotClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClusterSnapshotClosed
	}
	s.snapshot = cloneClusterStateSnapshot(snapshot)
	s.has = true
	return nil
}

func (s *InMemoryClusterSnapshotStore) Load(ctx context.Context) (ClusterStateSnapshot, bool, error) {
	if s == nil {
		return ClusterStateSnapshot{}, false, ErrClusterSnapshotClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ClusterStateSnapshot{}, false, ctx.Err()
	default:
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ClusterStateSnapshot{}, false, ErrClusterSnapshotClosed
	}
	if !s.has {
		return ClusterStateSnapshot{}, false, nil
	}
	return cloneClusterStateSnapshot(s.snapshot), true, nil
}

func (s *InMemoryClusterSnapshotStore) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true
	s.snapshot = ClusterStateSnapshot{}
	s.has = false
	return nil
}

func cloneClusterStateSnapshot(snapshot ClusterStateSnapshot) ClusterStateSnapshot {
	snapshot.Payload = cloneBytes(snapshot.Payload)
	snapshot.Timers = cloneClusterTimers(snapshot.Timers)
	return snapshot
}
