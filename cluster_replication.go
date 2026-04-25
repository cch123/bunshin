package bunshin

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const defaultClusterReplicationSyncInterval = 100 * time.Millisecond

var ErrClusterReplicationUnavailable = errors.New("bunshin cluster replication: unavailable")

type ClusterReplicationConfig struct {
	SourceLog      ClusterLog
	SyncInterval   time.Duration
	DisableAutoRun bool
}

type ClusterReplicationStatus struct {
	Enabled        bool      `json:"enabled"`
	SourcePosition int64     `json:"source_position"`
	SyncedPosition int64     `json:"synced_position"`
	Applied        uint64    `json:"applied"`
	LastSync       time.Time `json:"last_sync,omitempty"`
	LastError      string    `json:"last_error,omitempty"`
}

type ClusterReplicationSyncResult struct {
	SourcePosition   int64
	PreviousPosition int64
	SyncedPosition   int64
	Applied          uint64
}

type clusterReplicationState struct {
	sourceLog      ClusterLog
	syncInterval   time.Duration
	disableAutoRun bool
	position       int64
	sourcePosition int64
	applied        uint64
	lastSync       time.Time
	lastError      string
	cancel         context.CancelFunc
	done           chan struct{}
}

func (n *ClusterNode) SyncReplication(ctx context.Context) (ClusterReplicationSyncResult, error) {
	if n == nil {
		return ClusterReplicationSyncResult{}, ErrClusterClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}

	n.applyMu.Lock()
	defer n.applyMu.Unlock()

	result, err := n.syncReplicationLocked(ctx)
	n.recordReplicationSync(result, err)
	return result, err
}

func (n *ClusterNode) syncReplicationLocked(ctx context.Context) (ClusterReplicationSyncResult, error) {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return ClusterReplicationSyncResult{}, ErrClusterClosed
	}
	replication := n.replication
	if replication == nil {
		n.mu.Unlock()
		return ClusterReplicationSyncResult{}, ErrClusterReplicationUnavailable
	}
	nodeID := n.nodeID
	role := n.role
	position := replication.position
	n.mu.Unlock()

	entries, err := replication.sourceLog.Snapshot(ctx)
	if err != nil {
		return ClusterReplicationSyncResult{}, err
	}
	sourcePosition, err := replication.sourceLog.LastPosition(ctx)
	if err != nil {
		return ClusterReplicationSyncResult{}, err
	}
	result := ClusterReplicationSyncResult{
		SourcePosition:   sourcePosition,
		PreviousPosition: position,
		SyncedPosition:   position,
	}
	if sourcePosition < position {
		return result, fmt.Errorf("%w: source position=%d local position=%d", ErrClusterLogPosition, sourcePosition, position)
	}
	for _, entry := range entries {
		if entry.Position <= position {
			continue
		}
		appended, err := n.log.Append(ctx, cloneClusterLogEntry(entry))
		if err != nil {
			return result, err
		}
		if appended.Position != entry.Position {
			return result, fmt.Errorf("%w: replicated position=%d local position=%d", ErrClusterLogPosition, entry.Position, appended.Position)
		}
		if _, err := n.applyEntry(ctx, nodeID, role, appended, false); err != nil {
			return result, err
		}
		position = appended.Position
		result.SyncedPosition = position
		result.Applied++
	}
	return result, nil
}

func normalizeClusterReplicationConfig(cfg *ClusterReplicationConfig) (ClusterReplicationConfig, error) {
	if cfg == nil {
		return ClusterReplicationConfig{}, invalidConfigf("replication config is required")
	}
	normalized := *cfg
	if normalized.SourceLog == nil {
		return ClusterReplicationConfig{}, invalidConfigf("replication source log is required")
	}
	if normalized.SyncInterval < 0 {
		return ClusterReplicationConfig{}, invalidConfigf("invalid replication sync interval: %s", normalized.SyncInterval)
	}
	if normalized.SyncInterval == 0 {
		normalized.SyncInterval = defaultClusterReplicationSyncInterval
	}
	return normalized, nil
}

func newClusterReplicationState(cfg *ClusterReplicationConfig) *clusterReplicationState {
	if cfg == nil {
		return nil
	}
	return &clusterReplicationState{
		sourceLog:      cfg.SourceLog,
		syncInterval:   cfg.SyncInterval,
		disableAutoRun: cfg.DisableAutoRun,
	}
}

func (n *ClusterNode) startReplicationLoop() {
	n.mu.Lock()
	replication := n.replication
	if replication == nil || replication.disableAutoRun || replication.cancel != nil || n.role == ClusterRoleLeader {
		n.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	interval := replication.syncInterval
	replication.cancel = cancel
	replication.done = done
	n.mu.Unlock()

	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			if _, err := n.SyncReplication(ctx); err != nil && !errors.Is(err, context.Canceled) {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (n *ClusterNode) stopReplicationLoop() {
	n.mu.Lock()
	var (
		cancel context.CancelFunc
		done   chan struct{}
	)
	if n.replication != nil {
		cancel = n.replication.cancel
		done = n.replication.done
		n.replication.cancel = nil
		n.replication.done = nil
	}
	n.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (n *ClusterNode) setReplicationPosition(position int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.replication != nil {
		n.replication.position = position
	}
}

func (n *ClusterNode) recordReplicationSync(result ClusterReplicationSyncResult, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.replication == nil {
		return
	}
	if result.SyncedPosition > n.replication.position {
		n.replication.position = result.SyncedPosition
	}
	if result.SourcePosition > n.replication.sourcePosition {
		n.replication.sourcePosition = result.SourcePosition
	}
	n.replication.applied += result.Applied
	n.replication.lastSync = time.Now().UTC()
	if err != nil {
		n.replication.lastError = err.Error()
		return
	}
	n.replication.lastError = ""
}

func (n *ClusterNode) replicationStatusLocked() ClusterReplicationStatus {
	if n.replication == nil {
		return ClusterReplicationStatus{}
	}
	return ClusterReplicationStatus{
		Enabled:        true,
		SourcePosition: n.replication.sourcePosition,
		SyncedPosition: n.replication.position,
		Applied:        n.replication.applied,
		LastSync:       n.replication.lastSync,
		LastError:      n.replication.lastError,
	}
}
