package bunshin

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const defaultClusterLearnerSyncInterval = 100 * time.Millisecond

var ErrClusterLearnerUnavailable = errors.New("bunshin cluster learner: unavailable")

type ClusterLearnerConfig struct {
	MasterLog      ClusterLog
	SyncInterval   time.Duration
	SnapshotEvery  uint64
	DisableAutoRun bool
}

type ClusterLearnerStatus struct {
	Enabled              bool      `json:"enabled"`
	MasterPosition       int64     `json:"master_position"`
	SyncedPosition       int64     `json:"synced_position"`
	Snapshots            uint64    `json:"snapshots"`
	AppliedSinceSnapshot uint64    `json:"applied_since_snapshot"`
	LastSync             time.Time `json:"last_sync,omitempty"`
	LastError            string    `json:"last_error,omitempty"`
}

type ClusterLearnerSyncResult struct {
	MasterPosition   int64
	PreviousPosition int64
	SyncedPosition   int64
	Applied          uint64
	SnapshotTaken    bool
	Snapshot         ClusterStateSnapshot
}

type clusterLearnerState struct {
	masterLog            ClusterLog
	syncInterval         time.Duration
	snapshotEvery        uint64
	disableAutoRun       bool
	position             int64
	masterPosition       int64
	snapshots            uint64
	appliedSinceSnapshot uint64
	lastSync             time.Time
	lastError            string
	cancel               context.CancelFunc
	done                 chan struct{}
}

func (n *ClusterNode) SyncLearner(ctx context.Context) (ClusterLearnerSyncResult, error) {
	if n == nil {
		return ClusterLearnerSyncResult{}, ErrClusterClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}

	n.applyMu.Lock()
	defer n.applyMu.Unlock()

	result, err := n.syncLearnerLocked(ctx)
	n.recordLearnerSync(result, err)
	return result, err
}

func (n *ClusterNode) syncLearnerLocked(ctx context.Context) (ClusterLearnerSyncResult, error) {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return ClusterLearnerSyncResult{}, ErrClusterClosed
	}
	learner := n.learner
	if learner == nil {
		n.mu.Unlock()
		return ClusterLearnerSyncResult{}, ErrClusterLearnerUnavailable
	}
	nodeID := n.nodeID
	role := n.role
	position := learner.position
	n.mu.Unlock()

	entries, err := learner.masterLog.Snapshot(ctx)
	if err != nil {
		return ClusterLearnerSyncResult{}, err
	}
	masterPosition, err := learner.masterLog.LastPosition(ctx)
	if err != nil {
		return ClusterLearnerSyncResult{}, err
	}
	result := ClusterLearnerSyncResult{
		MasterPosition:   masterPosition,
		PreviousPosition: position,
		SyncedPosition:   position,
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

	takeSnapshot := false
	n.mu.Lock()
	if learner := n.learner; learner != nil {
		learner.position = position
		learner.masterPosition = masterPosition
		pending := learner.appliedSinceSnapshot + result.Applied
		takeSnapshot = pending > 0 && (learner.snapshotEvery == 0 || pending >= learner.snapshotEvery)
		learner.appliedSinceSnapshot = pending
	}
	n.mu.Unlock()

	if takeSnapshot {
		snapshot, err := n.takeSnapshot(ctx)
		if err != nil {
			return result, err
		}
		result.SnapshotTaken = true
		result.Snapshot = snapshot
		n.recordLearnerSnapshot(snapshot.Position)
	}
	return result, nil
}

func normalizeClusterLearnerConfig(cfg *ClusterLearnerConfig) (ClusterLearnerConfig, error) {
	if cfg == nil {
		return ClusterLearnerConfig{}, invalidConfigf("learner config is required")
	}
	normalized := *cfg
	if normalized.MasterLog == nil {
		return ClusterLearnerConfig{}, invalidConfigf("learner master log is required")
	}
	if normalized.SyncInterval < 0 {
		return ClusterLearnerConfig{}, invalidConfigf("invalid learner sync interval: %s", normalized.SyncInterval)
	}
	if normalized.SyncInterval == 0 {
		normalized.SyncInterval = defaultClusterLearnerSyncInterval
	}
	return normalized, nil
}

func newClusterLearnerState(cfg *ClusterLearnerConfig) *clusterLearnerState {
	if cfg == nil {
		return nil
	}
	return &clusterLearnerState{
		masterLog:      cfg.MasterLog,
		syncInterval:   cfg.SyncInterval,
		snapshotEvery:  cfg.SnapshotEvery,
		disableAutoRun: cfg.DisableAutoRun,
	}
}

func (n *ClusterNode) startLearnerLoop() {
	n.mu.Lock()
	learner := n.learner
	if learner == nil || learner.disableAutoRun || learner.cancel != nil {
		n.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	interval := learner.syncInterval
	learner.cancel = cancel
	learner.done = done
	n.mu.Unlock()

	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			if _, err := n.SyncLearner(ctx); err != nil && !errors.Is(err, context.Canceled) {
				n.recordLearnerSync(ClusterLearnerSyncResult{}, err)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (n *ClusterNode) stopLearnerLoop() {
	n.mu.Lock()
	var (
		cancel context.CancelFunc
		done   chan struct{}
	)
	if n.learner != nil {
		cancel = n.learner.cancel
		done = n.learner.done
		n.learner.cancel = nil
		n.learner.done = nil
	}
	n.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (n *ClusterNode) setLearnerPosition(position int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.learner != nil {
		n.learner.position = position
	}
}

func (n *ClusterNode) recordLearnerSnapshot(position int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.learner == nil {
		return
	}
	n.learner.position = position
	n.learner.snapshots++
	n.learner.appliedSinceSnapshot = 0
}

func (n *ClusterNode) recordLearnerSync(result ClusterLearnerSyncResult, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.learner == nil {
		return
	}
	if result.SyncedPosition > n.learner.position {
		n.learner.position = result.SyncedPosition
	}
	if result.MasterPosition > n.learner.masterPosition {
		n.learner.masterPosition = result.MasterPosition
	}
	n.learner.lastSync = time.Now().UTC()
	if err != nil {
		n.learner.lastError = err.Error()
		return
	}
	n.learner.lastError = ""
}

func (n *ClusterNode) learnerStatusLocked() ClusterLearnerStatus {
	if n.learner == nil {
		return ClusterLearnerStatus{}
	}
	return ClusterLearnerStatus{
		Enabled:              true,
		MasterPosition:       n.learner.masterPosition,
		SyncedPosition:       n.learner.position,
		Snapshots:            n.learner.snapshots,
		AppliedSinceSnapshot: n.learner.appliedSinceSnapshot,
		LastSync:             n.learner.lastSync,
		LastError:            n.learner.lastError,
	}
}
