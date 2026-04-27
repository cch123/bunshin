package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const defaultClusterBackupSyncInterval = 100 * time.Millisecond

var (
	ErrClusterBackupClosed      = errors.New("bunshin cluster backup: closed")
	ErrClusterBackupUnsupported = errors.New("bunshin cluster backup: service unsupported")
)

type ClusterBackupConfig struct {
	NodeID         ClusterNodeID
	SourceLog      ClusterLog
	Log            ClusterLog
	SnapshotStore  ClusterSnapshotStore
	Service        ClusterService
	Standby        bool
	SyncInterval   time.Duration
	SnapshotEvery  uint64
	DisableAutoRun bool
	Logger         Logger
}

type ClusterBackup struct {
	mu                   sync.Mutex
	applyMu              sync.Mutex
	nodeID               ClusterNodeID
	role                 ClusterRole
	sourceLog            ClusterLog
	log                  ClusterLog
	snapshotStore        ClusterSnapshotStore
	service              ClusterService
	snapshotService      ClusterSnapshotService
	standby              bool
	syncInterval         time.Duration
	snapshotEvery        uint64
	logger               Logger
	position             int64
	sourcePosition       int64
	applied              uint64
	snapshots            uint64
	appliedSinceSnapshot uint64
	timers               map[ClusterTimerID]ClusterTimer
	lastSync             time.Time
	lastError            string
	closed               bool
	cancel               context.CancelFunc
	done                 chan struct{}
}

type ClusterBackupStatus struct {
	NodeID               ClusterNodeID `json:"node_id"`
	Role                 ClusterRole   `json:"role"`
	Standby              bool          `json:"standby"`
	Closed               bool          `json:"closed"`
	SourcePosition       int64         `json:"source_position"`
	SyncedPosition       int64         `json:"synced_position"`
	Applied              uint64        `json:"applied"`
	Snapshots            uint64        `json:"snapshots"`
	AppliedSinceSnapshot uint64        `json:"applied_since_snapshot"`
	LastSync             time.Time     `json:"last_sync,omitempty"`
	LastError            string        `json:"last_error,omitempty"`
}

type ClusterBackupSyncResult struct {
	SourcePosition   int64
	PreviousPosition int64
	SyncedPosition   int64
	Copied           uint64
	Applied          uint64
	SnapshotTaken    bool
	Snapshot         ClusterStateSnapshot
}

func StartClusterBackup(ctx context.Context, cfg ClusterBackupConfig) (*ClusterBackup, error) {
	normalized, err := normalizeClusterBackupConfig(cfg)
	if err != nil {
		return nil, err
	}
	backup := &ClusterBackup{
		nodeID:        normalized.NodeID,
		role:          clusterBackupRole(normalized.Standby),
		sourceLog:     normalized.SourceLog,
		log:           normalized.Log,
		snapshotStore: normalized.SnapshotStore,
		service:       normalized.Service,
		standby:       normalized.Standby,
		syncInterval:  normalized.SyncInterval,
		snapshotEvery: normalized.SnapshotEvery,
		logger:        normalized.Logger,
		timers:        make(map[ClusterTimerID]ClusterTimer),
	}
	if normalized.SnapshotStore != nil {
		backup.snapshotService = normalized.Service.(ClusterSnapshotService)
	}
	if err := backup.start(ctx); err != nil {
		return nil, err
	}
	if !normalized.DisableAutoRun {
		backup.startLoop()
	}
	return backup, nil
}

func (b *ClusterBackup) Sync(ctx context.Context) (ClusterBackupSyncResult, error) {
	if b == nil {
		return ClusterBackupSyncResult{}, ErrClusterBackupClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}

	b.applyMu.Lock()
	defer b.applyMu.Unlock()

	result, err := b.syncLocked(ctx)
	b.recordSync(result, err)
	return result, err
}

func (b *ClusterBackup) Snapshot(ctx context.Context) (ClusterBackupStatus, error) {
	if b == nil {
		return ClusterBackupStatus{}, ErrClusterBackupClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ClusterBackupStatus{}, ctx.Err()
	default:
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.statusLocked(), nil
}

func (b *ClusterBackup) Close() error {
	if b == nil {
		return nil
	}
	b.stopLoop()
	b.applyMu.Lock()
	defer b.applyMu.Unlock()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}

func (b *ClusterBackup) start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	b.applyMu.Lock()
	defer b.applyMu.Unlock()

	snapshotPosition, err := b.loadSnapshot(ctx)
	if err != nil {
		return err
	}
	if b.standby && b.service != nil {
		if lifecycle, ok := b.service.(ClusterLifecycleService); ok {
			position, err := b.log.LastPosition(ctx)
			if err != nil {
				return err
			}
			if err := lifecycle.OnClusterStart(ctx, ClusterServiceContext{
				NodeID:           b.nodeID,
				Role:             b.role,
				LastPosition:     position,
				SnapshotPosition: snapshotPosition,
			}); err != nil {
				return err
			}
		}
	}
	entries, err := b.log.Snapshot(ctx)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Position <= snapshotPosition {
			continue
		}
		if _, err := b.applyEntry(ctx, entry, true); err != nil {
			return err
		}
	}
	position, err := b.log.LastPosition(ctx)
	if err != nil {
		return err
	}
	if position < snapshotPosition {
		position = snapshotPosition
	}
	b.mu.Lock()
	b.position = position
	b.mu.Unlock()
	return nil
}

func (b *ClusterBackup) syncLocked(ctx context.Context) (ClusterBackupSyncResult, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ClusterBackupSyncResult{}, ErrClusterBackupClosed
	}
	position := b.position
	b.mu.Unlock()

	entries, err := b.sourceLog.Snapshot(ctx)
	if err != nil {
		return ClusterBackupSyncResult{}, err
	}
	sourcePosition, err := b.sourceLog.LastPosition(ctx)
	if err != nil {
		return ClusterBackupSyncResult{}, err
	}
	result := ClusterBackupSyncResult{
		SourcePosition:   sourcePosition,
		PreviousPosition: position,
		SyncedPosition:   position,
	}
	if sourcePosition < position {
		return result, fmt.Errorf("%w: source position=%d backup position=%d", ErrClusterLogPosition, sourcePosition, position)
	}
	for _, entry := range entries {
		if entry.Position <= position {
			continue
		}
		appended, err := b.log.Append(ctx, cloneClusterLogEntry(entry))
		if err != nil {
			return result, err
		}
		if appended.Position != entry.Position {
			return result, fmt.Errorf("%w: replicated position=%d local position=%d", ErrClusterLogPosition, entry.Position, appended.Position)
		}
		egress, err := b.applyEntry(ctx, appended, false)
		if err != nil {
			return result, err
		}
		position = appended.Position
		result.SyncedPosition = position
		result.Copied++
		if b.standby && clusterLogEntryDeliversToService(egress.Type) {
			result.Applied++
		}
	}

	takeSnapshot := false
	b.mu.Lock()
	pending := b.appliedSinceSnapshot + result.Applied
	if b.snapshotStore != nil && result.Copied > 0 && (b.snapshotEvery == 0 || pending >= b.snapshotEvery) {
		takeSnapshot = true
	}
	b.position = position
	b.sourcePosition = sourcePosition
	b.appliedSinceSnapshot = pending
	b.mu.Unlock()

	if takeSnapshot {
		snapshot, err := b.takeSnapshot(ctx)
		if err != nil {
			return result, err
		}
		result.SnapshotTaken = true
		result.Snapshot = snapshot
	}
	return result, nil
}

func (b *ClusterBackup) applyEntry(ctx context.Context, entry ClusterLogEntry, replay bool) (ClusterEgress, error) {
	entryType := clusterLogEntryType(entry.Type)
	switch entryType {
	case ClusterLogEntryTimerSchedule:
		b.applyTimerSchedule(entry)
		return clusterEgressFromEntry(entry, nil), nil
	case ClusterLogEntryTimerCancel:
		b.applyTimerCancel(entry.TimerID)
		return clusterEgressFromEntry(entry, nil), nil
	case ClusterLogEntryTimerFire:
		b.applyTimerFire(entry.TimerID)
	case ClusterLogEntryIngress, ClusterLogEntryServiceMessage:
	default:
		return ClusterEgress{}, ErrClusterLogEntryType
	}
	if !b.standby || b.service == nil {
		return clusterEgressFromEntry(entry, nil), nil
	}
	payload, err := b.service.OnClusterMessage(ctx, ClusterMessage{
		NodeID:        b.nodeID,
		Role:          b.role,
		Type:          entryType,
		SessionID:     entry.SessionID,
		CorrelationID: entry.CorrelationID,
		TimerID:       entry.TimerID,
		Deadline:      entry.Deadline,
		SourceService: entry.SourceService,
		TargetService: entry.TargetService,
		LogPosition:   entry.Position,
		Replay:        replay,
		Payload:       cloneBytes(entry.Payload),
	})
	if err != nil {
		return ClusterEgress{}, err
	}
	return clusterEgressFromEntry(entry, payload), nil
}

func (b *ClusterBackup) takeSnapshot(ctx context.Context) (ClusterStateSnapshot, error) {
	if b.snapshotStore == nil || b.snapshotService == nil {
		return ClusterStateSnapshot{}, ErrClusterSnapshotStoreUnavailable
	}
	position, err := b.log.LastPosition(ctx)
	if err != nil {
		return ClusterStateSnapshot{}, err
	}
	payload, err := b.snapshotService.SnapshotClusterState(ctx, ClusterServiceContext{
		NodeID:           b.nodeID,
		Role:             b.role,
		LastPosition:     position,
		SnapshotPosition: b.snapshotPosition(ctx),
	})
	if err != nil {
		return ClusterStateSnapshot{}, err
	}
	snapshot := ClusterStateSnapshot{
		NodeID:   b.nodeID,
		Role:     b.role,
		Position: position,
		TakenAt:  time.Now().UTC(),
		Payload:  cloneBytes(payload),
		Timers:   b.snapshotTimers(),
	}
	if err := b.snapshotStore.Save(ctx, snapshot); err != nil {
		return ClusterStateSnapshot{}, err
	}
	b.mu.Lock()
	b.snapshots++
	b.appliedSinceSnapshot = 0
	b.mu.Unlock()
	return cloneClusterStateSnapshot(snapshot), nil
}

func (b *ClusterBackup) loadSnapshot(ctx context.Context) (int64, error) {
	if b.snapshotStore == nil {
		return 0, nil
	}
	snapshot, ok, err := b.snapshotStore.Load(ctx)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	if b.snapshotService == nil {
		return 0, ErrClusterBackupUnsupported
	}
	if err := b.snapshotService.LoadClusterSnapshot(ctx, cloneClusterStateSnapshot(snapshot)); err != nil {
		return 0, err
	}
	b.restoreTimers(snapshot.Timers)
	return snapshot.Position, nil
}

func normalizeClusterBackupConfig(cfg ClusterBackupConfig) (ClusterBackupConfig, error) {
	if cfg.NodeID == 0 {
		cfg.NodeID = defaultClusterNodeID
	}
	if cfg.SourceLog == nil {
		return ClusterBackupConfig{}, invalidConfigf("cluster backup source log is required")
	}
	if cfg.Log == nil {
		cfg.Log = NewInMemoryClusterLog()
	}
	if cfg.SyncInterval < 0 {
		return ClusterBackupConfig{}, invalidConfigf("invalid cluster backup sync interval: %s", cfg.SyncInterval)
	}
	if cfg.SyncInterval == 0 {
		cfg.SyncInterval = defaultClusterBackupSyncInterval
	}
	if cfg.SnapshotStore != nil {
		if cfg.Service == nil {
			return ClusterBackupConfig{}, invalidConfigf("cluster backup snapshot service is required")
		}
		if _, ok := cfg.Service.(ClusterSnapshotService); !ok {
			return ClusterBackupConfig{}, ErrClusterBackupUnsupported
		}
	}
	if cfg.Standby && cfg.Service == nil {
		return ClusterBackupConfig{}, invalidConfigf("cluster standby service is required")
	}
	return cfg, nil
}

func (b *ClusterBackup) startLoop() {
	b.mu.Lock()
	if b.cancel != nil || b.closed {
		b.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	interval := b.syncInterval
	b.cancel = cancel
	b.done = done
	b.mu.Unlock()

	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			if _, err := b.Sync(ctx); err != nil && !errors.Is(err, context.Canceled) {
				b.recordSync(ClusterBackupSyncResult{}, err)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (b *ClusterBackup) stopLoop() {
	b.mu.Lock()
	cancel := b.cancel
	done := b.done
	b.cancel = nil
	b.done = nil
	b.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (b *ClusterBackup) recordSync(result ClusterBackupSyncResult, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if result.SyncedPosition > b.position {
		b.position = result.SyncedPosition
	}
	if result.SourcePosition > b.sourcePosition {
		b.sourcePosition = result.SourcePosition
	}
	b.applied += result.Applied
	b.lastSync = time.Now().UTC()
	if err != nil {
		b.lastError = err.Error()
		return
	}
	b.lastError = ""
}

func (b *ClusterBackup) statusLocked() ClusterBackupStatus {
	return ClusterBackupStatus{
		NodeID:               b.nodeID,
		Role:                 b.role,
		Standby:              b.standby,
		Closed:               b.closed,
		SourcePosition:       b.sourcePosition,
		SyncedPosition:       b.position,
		Applied:              b.applied,
		Snapshots:            b.snapshots,
		AppliedSinceSnapshot: b.appliedSinceSnapshot,
		LastSync:             b.lastSync,
		LastError:            b.lastError,
	}
}

func (b *ClusterBackup) snapshotPosition(ctx context.Context) int64 {
	if b.snapshotStore == nil {
		return 0
	}
	snapshot, ok, err := b.snapshotStore.Load(ctx)
	if err != nil || !ok {
		return 0
	}
	return snapshot.Position
}

func (b *ClusterBackup) applyTimerSchedule(entry ClusterLogEntry) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ensureTimerMapLocked()
	b.timers[entry.TimerID] = ClusterTimer{
		TimerID:  entry.TimerID,
		Deadline: entry.Deadline.UTC(),
		Payload:  cloneBytes(entry.Payload),
	}
}

func (b *ClusterBackup) applyTimerCancel(timerID ClusterTimerID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.timers, timerID)
}

func (b *ClusterBackup) applyTimerFire(timerID ClusterTimerID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.timers, timerID)
}

func (b *ClusterBackup) snapshotTimers() []ClusterTimer {
	b.mu.Lock()
	defer b.mu.Unlock()
	timers := make([]ClusterTimer, 0, len(b.timers))
	for _, timer := range b.timers {
		timers = append(timers, cloneClusterTimer(timer))
	}
	sortClusterTimers(timers)
	return timers
}

func (b *ClusterBackup) restoreTimers(timers []ClusterTimer) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.timers = make(map[ClusterTimerID]ClusterTimer, len(timers))
	for _, timer := range timers {
		timer = cloneClusterTimer(timer)
		timer.Deadline = timer.Deadline.UTC()
		b.timers[timer.TimerID] = timer
	}
}

func (b *ClusterBackup) ensureTimerMapLocked() {
	if b.timers == nil {
		b.timers = make(map[ClusterTimerID]ClusterTimer)
	}
}

func clusterBackupRole(standby bool) ClusterRole {
	if standby {
		return ClusterRoleStandby
	}
	return ClusterRoleBackup
}

func clusterLogEntryDeliversToService(entryType ClusterLogEntryType) bool {
	switch clusterLogEntryType(entryType) {
	case ClusterLogEntryIngress, ClusterLogEntryServiceMessage, ClusterLogEntryTimerFire:
		return true
	default:
		return false
	}
}
