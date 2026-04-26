package bunshin

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

const (
	clusterArchiveLogStreamID      uint32 = 0x434c4f47
	clusterArchiveSnapshotStreamID uint32 = 0x43534e50
	clusterArchiveSessionID        uint32 = 1
)

type ArchiveClusterLog struct {
	mu      sync.Mutex
	archive *Archive
	closed  bool
}

type ArchiveClusterSnapshotStore struct {
	mu      sync.Mutex
	archive *Archive
	closed  bool
}

type archiveClusterLogRecord struct {
	Entry ClusterLogEntry `json:"entry"`
}

type archiveClusterSnapshotRecord struct {
	Snapshot ClusterStateSnapshot `json:"snapshot"`
}

func NewArchiveClusterLog(archive *Archive) (*ArchiveClusterLog, error) {
	if archive == nil {
		return nil, invalidConfigf("archive is required")
	}
	return &ArchiveClusterLog{archive: archive}, nil
}

func NewArchiveClusterSnapshotStore(archive *Archive) (*ArchiveClusterSnapshotStore, error) {
	if archive == nil {
		return nil, invalidConfigf("archive is required")
	}
	return &ArchiveClusterSnapshotStore{archive: archive}, nil
}

func (l *ArchiveClusterLog) Append(ctx context.Context, entry ClusterLogEntry) (ClusterLogEntry, error) {
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
	lastPosition, err := l.lastPositionLocked(ctx)
	if err != nil {
		return ClusterLogEntry{}, err
	}
	nextPosition := lastPosition + 1
	if entry.Position == 0 {
		entry.Position = nextPosition
	} else if entry.Position != nextPosition {
		return ClusterLogEntry{}, ErrClusterLogPosition
	}
	entry.Payload = cloneBytes(entry.Payload)
	payload, err := json.Marshal(archiveClusterLogRecord{Entry: entry})
	if err != nil {
		return ClusterLogEntry{}, fmt.Errorf("encode archive cluster log entry: %w", err)
	}
	if _, err := l.archive.Record(Message{
		StreamID:  clusterArchiveLogStreamID,
		SessionID: clusterArchiveSessionID,
		Sequence:  uint64(entry.Position),
		Payload:   payload,
	}); err != nil {
		return ClusterLogEntry{}, err
	}
	return cloneClusterLogEntry(entry), nil
}

func (l *ArchiveClusterLog) Snapshot(ctx context.Context) ([]ClusterLogEntry, error) {
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
	return l.snapshotLocked(ctx)
}

func (l *ArchiveClusterLog) LastPosition(ctx context.Context) (int64, error) {
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
	return l.lastPositionLocked(ctx)
}

func (l *ArchiveClusterLog) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	l.closed = true
	return nil
}

func (l *ArchiveClusterLog) snapshotLocked(ctx context.Context) ([]ClusterLogEntry, error) {
	var entries []ClusterLogEntry
	err := l.archive.Replay(ctx, ArchiveReplayConfig{
		StreamID:  clusterArchiveLogStreamID,
		SessionID: clusterArchiveSessionID,
	}, func(_ context.Context, msg Message) error {
		var record archiveClusterLogRecord
		if err := json.Unmarshal(msg.Payload, &record); err != nil {
			return fmt.Errorf("decode archive cluster log entry: %w", err)
		}
		entries = append(entries, cloneClusterLogEntry(record.Entry))
		return nil
	})
	return entries, err
}

func (l *ArchiveClusterLog) lastPositionLocked(ctx context.Context) (int64, error) {
	var position int64
	err := l.archive.Replay(ctx, ArchiveReplayConfig{
		StreamID:  clusterArchiveLogStreamID,
		SessionID: clusterArchiveSessionID,
	}, func(_ context.Context, msg Message) error {
		var record archiveClusterLogRecord
		if err := json.Unmarshal(msg.Payload, &record); err != nil {
			return fmt.Errorf("decode archive cluster log entry: %w", err)
		}
		if record.Entry.Position > position {
			position = record.Entry.Position
		}
		return nil
	})
	return position, err
}

func (s *ArchiveClusterSnapshotStore) Save(ctx context.Context, snapshot ClusterStateSnapshot) error {
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
	snapshot = cloneClusterStateSnapshot(snapshot)
	payload, err := json.Marshal(archiveClusterSnapshotRecord{Snapshot: snapshot})
	if err != nil {
		return fmt.Errorf("encode archive cluster snapshot: %w", err)
	}
	_, err = s.archive.Record(Message{
		StreamID:  clusterArchiveSnapshotStreamID,
		SessionID: clusterArchiveSessionID,
		Sequence:  uint64(snapshot.Position),
		Payload:   payload,
	})
	return err
}

func (s *ArchiveClusterSnapshotStore) Load(ctx context.Context) (ClusterStateSnapshot, bool, error) {
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
	var (
		snapshot ClusterStateSnapshot
		ok       bool
	)
	err := s.archive.Replay(ctx, ArchiveReplayConfig{
		StreamID:  clusterArchiveSnapshotStreamID,
		SessionID: clusterArchiveSessionID,
	}, func(_ context.Context, msg Message) error {
		var record archiveClusterSnapshotRecord
		if err := json.Unmarshal(msg.Payload, &record); err != nil {
			return fmt.Errorf("decode archive cluster snapshot: %w", err)
		}
		if !ok || record.Snapshot.Position >= snapshot.Position {
			snapshot = cloneClusterStateSnapshot(record.Snapshot)
			ok = true
		}
		return nil
	})
	if err != nil {
		return ClusterStateSnapshot{}, false, err
	}
	return snapshot, ok, nil
}

func (s *ArchiveClusterSnapshotStore) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true
	return nil
}
