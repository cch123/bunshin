package bunshin

import (
	"context"
	"errors"
	"sync"
	"time"
)

const (
	defaultClusterNodeID ClusterNodeID = 1
)

var (
	ErrClusterClosed             = errors.New("bunshin cluster: closed")
	ErrClusterSuspended          = errors.New("bunshin cluster: suspended")
	ErrClusterNotLeader          = errors.New("bunshin cluster: not leader")
	ErrClusterServiceUnavailable = errors.New("bunshin cluster: service unavailable")
	ErrClusterLogClosed          = errors.New("bunshin cluster log: closed")
	ErrClusterLogPosition        = errors.New("bunshin cluster log: invalid position")
	ErrClusterLogEntryType       = errors.New("bunshin cluster log: invalid entry type")
)

type ClusterNodeID uint64

type ClusterSessionID uint64

type ClusterCorrelationID uint64

type ClusterMode string

const (
	ClusterModeSingleNode      ClusterMode = "single-node"
	ClusterModeAppointedLeader ClusterMode = "appointed-leader"
	ClusterModeLearner         ClusterMode = "learner"
)

type ClusterRole string

const (
	ClusterRoleLeader   ClusterRole = "leader"
	ClusterRoleFollower ClusterRole = "follower"
	ClusterRoleLearner  ClusterRole = "learner"
)

type ClusterConfig struct {
	NodeID             ClusterNodeID
	Mode               ClusterMode
	AppointedLeaderID  ClusterNodeID
	Log                ClusterLog
	SnapshotStore      ClusterSnapshotStore
	Learner            *ClusterLearnerConfig
	Replication        *ClusterReplicationConfig
	Election           *ClusterElectionConfig
	TimerCheckInterval time.Duration
	DisableTimerLoop   bool
	Service            ClusterService
	Logger             Logger
}

type ClusterNode struct {
	mu                 sync.Mutex
	applyMu            sync.Mutex
	nodeID             ClusterNodeID
	mode               ClusterMode
	role               ClusterRole
	log                ClusterLog
	snapshotStore      ClusterSnapshotStore
	learner            *clusterLearnerState
	replication        *clusterReplicationState
	election           *clusterElectionState
	timers             map[ClusterTimerID]ClusterTimer
	timerCheckInterval time.Duration
	disableTimerLoop   bool
	timerCancel        context.CancelFunc
	timerDone          chan struct{}
	service            ClusterService
	logger             Logger
	closed             bool
	suspended          bool
	nextSessionID      ClusterSessionID
	nextCorrelationID  ClusterCorrelationID
	nextTimerID        ClusterTimerID
}

type ClusterService interface {
	OnClusterMessage(context.Context, ClusterMessage) ([]byte, error)
}

type ClusterHandler func(context.Context, ClusterMessage) ([]byte, error)

func (f ClusterHandler) OnClusterMessage(ctx context.Context, msg ClusterMessage) ([]byte, error) {
	return f(ctx, msg)
}

type ClusterLifecycleService interface {
	OnClusterStart(context.Context, ClusterServiceContext) error
	OnClusterStop(context.Context, ClusterServiceContext) error
}

type ClusterServiceContext struct {
	NodeID           ClusterNodeID
	Role             ClusterRole
	LastPosition     int64
	SnapshotPosition int64
}

type ClusterIngress struct {
	SessionID     ClusterSessionID
	CorrelationID ClusterCorrelationID
	Payload       []byte
}

type ClusterEgress struct {
	SessionID     ClusterSessionID
	CorrelationID ClusterCorrelationID
	LogPosition   int64
	Type          ClusterLogEntryType
	TimerID       ClusterTimerID
	Deadline      time.Time
	SourceService string
	TargetService string
	Payload       []byte
}

type ClusterMessage struct {
	NodeID        ClusterNodeID
	Role          ClusterRole
	Type          ClusterLogEntryType
	SessionID     ClusterSessionID
	CorrelationID ClusterCorrelationID
	TimerID       ClusterTimerID
	Deadline      time.Time
	SourceService string
	TargetService string
	LogPosition   int64
	Replay        bool
	Payload       []byte
}

type ClusterSnapshot struct {
	NodeID           ClusterNodeID
	Mode             ClusterMode
	Role             ClusterRole
	LastPosition     int64
	SnapshotPosition int64
	Closed           bool
	Suspended        bool
	Learner          ClusterLearnerStatus
	Replication      ClusterReplicationStatus
	Election         ClusterElectionStatus
}

type ClusterClient struct {
	mu                sync.Mutex
	node              *ClusterNode
	sessionID         ClusterSessionID
	nextCorrelationID ClusterCorrelationID
}

func StartClusterNode(ctx context.Context, cfg ClusterConfig) (*ClusterNode, error) {
	normalized, err := normalizeClusterConfig(cfg)
	if err != nil {
		return nil, err
	}

	node := &ClusterNode{
		nodeID:             normalized.NodeID,
		mode:               normalized.Mode,
		role:               clusterRoleForConfig(normalized),
		log:                normalized.Log,
		snapshotStore:      normalized.SnapshotStore,
		learner:            newClusterLearnerState(normalized.Learner),
		replication:        newClusterReplicationState(normalized.Replication),
		election:           newClusterElectionState(normalized.Election, normalized.AppointedLeaderID),
		timers:             make(map[ClusterTimerID]ClusterTimer),
		timerCheckInterval: normalized.TimerCheckInterval,
		disableTimerLoop:   normalized.DisableTimerLoop,
		service:            normalized.Service,
		logger:             normalized.Logger,
		nextSessionID:      1,
		nextCorrelationID:  1,
		nextTimerID:        1,
	}
	if err := node.start(ctx); err != nil {
		return nil, err
	}
	node.startElectionLoop()
	node.startTimerLoop()
	node.startLearnerLoop()
	node.startReplicationLoop()
	return node, nil
}

func (n *ClusterNode) NodeID() ClusterNodeID {
	if n == nil {
		return 0
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.nodeID
}

func (n *ClusterNode) Role() ClusterRole {
	if n == nil {
		return ""
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

func (n *ClusterNode) Snapshot(ctx context.Context) (ClusterSnapshot, error) {
	if n == nil {
		return ClusterSnapshot{}, ErrClusterClosed
	}
	n.mu.Lock()
	snapshot := ClusterSnapshot{
		NodeID:      n.nodeID,
		Mode:        n.mode,
		Role:        n.role,
		Closed:      n.closed,
		Suspended:   n.suspended,
		Learner:     n.learnerStatusLocked(),
		Replication: n.replicationStatusLocked(),
		Election:    n.electionStatusLocked(time.Now().UTC()),
	}
	n.mu.Unlock()

	position, err := n.log.LastPosition(ctx)
	if err != nil {
		return ClusterSnapshot{}, err
	}
	snapshot.LastPosition = position
	if n.snapshotStore != nil {
		state, ok, err := n.snapshotStore.Load(ctx)
		if err != nil {
			return ClusterSnapshot{}, err
		}
		if ok {
			snapshot.SnapshotPosition = state.Position
		}
	}
	return snapshot, nil
}

func (n *ClusterNode) NewClient(ctx context.Context) (*ClusterClient, error) {
	if n == nil {
		return nil, ErrClusterClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.closed {
		return nil, ErrClusterClosed
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	sessionID := n.nextSessionID
	n.nextSessionID++
	return &ClusterClient{
		node:              n,
		sessionID:         sessionID,
		nextCorrelationID: 1,
	}, nil
}

func (n *ClusterNode) Submit(ctx context.Context, ingress ClusterIngress) (ClusterEgress, error) {
	if n == nil {
		return ClusterEgress{}, ErrClusterClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}

	n.applyMu.Lock()
	defer n.applyMu.Unlock()

	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return ClusterEgress{}, ErrClusterClosed
	}
	if n.suspended {
		n.mu.Unlock()
		return ClusterEgress{}, ErrClusterSuspended
	}
	if n.role != ClusterRoleLeader {
		n.mu.Unlock()
		return ClusterEgress{}, ErrClusterNotLeader
	}
	if ingress.SessionID == 0 {
		ingress.SessionID = n.nextSessionID
		n.nextSessionID++
	}
	if ingress.CorrelationID == 0 {
		ingress.CorrelationID = n.nextCorrelationID
		n.nextCorrelationID++
	}
	nodeID := n.nodeID
	role := n.role
	n.mu.Unlock()

	entry, err := n.log.Append(ctx, ClusterLogEntry{
		Type:          ClusterLogEntryIngress,
		SessionID:     ingress.SessionID,
		CorrelationID: ingress.CorrelationID,
		Payload:       cloneBytes(ingress.Payload),
	})
	if err != nil {
		return ClusterEgress{}, err
	}
	return n.applyEntry(ctx, nodeID, role, entry, false)
}

func (n *ClusterNode) Suspend(ctx context.Context) error {
	if n == nil {
		return ErrClusterClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	n.applyMu.Lock()
	defer n.applyMu.Unlock()

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.closed {
		return ErrClusterClosed
	}
	n.suspended = true
	return nil
}

func (n *ClusterNode) Resume(ctx context.Context) error {
	if n == nil {
		return ErrClusterClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	n.applyMu.Lock()
	defer n.applyMu.Unlock()

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.closed {
		return ErrClusterClosed
	}
	n.suspended = false
	return nil
}

func (n *ClusterNode) Close(ctx context.Context) error {
	if n == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	n.stopTimerLoop()
	n.stopElectionLoop()
	n.stopLearnerLoop()
	n.stopReplicationLoop()

	n.applyMu.Lock()
	defer n.applyMu.Unlock()

	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil
	}
	n.closed = true
	nodeID := n.nodeID
	role := n.role
	n.mu.Unlock()

	if lifecycle, ok := n.service.(ClusterLifecycleService); ok {
		position, err := n.log.LastPosition(ctx)
		if err != nil {
			return err
		}
		return lifecycle.OnClusterStop(ctx, ClusterServiceContext{
			NodeID:           nodeID,
			Role:             role,
			LastPosition:     position,
			SnapshotPosition: n.snapshotPosition(ctx),
		})
	}
	return nil
}

func (n *ClusterNode) TakeSnapshot(ctx context.Context) (ClusterStateSnapshot, error) {
	if n == nil {
		return ClusterStateSnapshot{}, ErrClusterClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}

	n.applyMu.Lock()
	defer n.applyMu.Unlock()

	return n.takeSnapshot(ctx)
}

func (n *ClusterNode) takeSnapshot(ctx context.Context) (ClusterStateSnapshot, error) {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return ClusterStateSnapshot{}, ErrClusterClosed
	}
	nodeID := n.nodeID
	role := n.role
	n.mu.Unlock()

	if n.snapshotStore == nil {
		return ClusterStateSnapshot{}, ErrClusterSnapshotStoreUnavailable
	}
	service, ok := n.service.(ClusterSnapshotService)
	if !ok {
		return ClusterStateSnapshot{}, ErrClusterSnapshotUnsupported
	}
	position, err := n.log.LastPosition(ctx)
	if err != nil {
		return ClusterStateSnapshot{}, err
	}
	payload, err := service.SnapshotClusterState(ctx, ClusterServiceContext{
		NodeID:           nodeID,
		Role:             role,
		LastPosition:     position,
		SnapshotPosition: n.snapshotPosition(ctx),
	})
	if err != nil {
		return ClusterStateSnapshot{}, err
	}
	snapshot := ClusterStateSnapshot{
		NodeID:   nodeID,
		Role:     role,
		Position: position,
		TakenAt:  time.Now().UTC(),
		Payload:  cloneBytes(payload),
		Timers:   n.snapshotTimers(),
	}
	if err := n.snapshotStore.Save(ctx, snapshot); err != nil {
		return ClusterStateSnapshot{}, err
	}
	return cloneClusterStateSnapshot(snapshot), nil
}

func (c *ClusterClient) SessionID() ClusterSessionID {
	if c == nil {
		return 0
	}
	return c.sessionID
}

func (c *ClusterClient) Send(ctx context.Context, payload []byte) (ClusterEgress, error) {
	if c == nil || c.node == nil {
		return ClusterEgress{}, ErrClusterClosed
	}
	c.mu.Lock()
	correlationID := c.nextCorrelationID
	c.nextCorrelationID++
	c.mu.Unlock()
	return c.node.Submit(ctx, ClusterIngress{
		SessionID:     c.sessionID,
		CorrelationID: correlationID,
		Payload:       payload,
	})
}

func (n *ClusterNode) start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	n.applyMu.Lock()
	defer n.applyMu.Unlock()

	snapshotPosition, err := n.loadSnapshot(ctx)
	if err != nil {
		return err
	}

	if lifecycle, ok := n.service.(ClusterLifecycleService); ok {
		position, err := n.log.LastPosition(ctx)
		if err != nil {
			return err
		}
		if err := lifecycle.OnClusterStart(ctx, ClusterServiceContext{
			NodeID:           n.nodeID,
			Role:             n.role,
			LastPosition:     position,
			SnapshotPosition: snapshotPosition,
		}); err != nil {
			return err
		}
	}

	entries, err := n.log.Snapshot(ctx)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Position <= snapshotPosition {
			continue
		}
		if _, err := n.applyEntry(ctx, n.nodeID, n.role, entry, true); err != nil {
			return err
		}
	}
	if n.learner != nil {
		position, err := n.log.LastPosition(ctx)
		if err != nil {
			return err
		}
		if position < snapshotPosition {
			position = snapshotPosition
		}
		n.setLearnerPosition(position)
	}
	if n.replication != nil {
		position, err := n.log.LastPosition(ctx)
		if err != nil {
			return err
		}
		if position < snapshotPosition {
			position = snapshotPosition
		}
		n.setReplicationPosition(position)
	}
	return nil
}

func (n *ClusterNode) loadSnapshot(ctx context.Context) (int64, error) {
	if n.snapshotStore == nil {
		return 0, nil
	}
	snapshot, ok, err := n.snapshotStore.Load(ctx)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	service, supported := n.service.(ClusterSnapshotService)
	if !supported {
		return 0, ErrClusterSnapshotUnsupported
	}
	if err := service.LoadClusterSnapshot(ctx, cloneClusterStateSnapshot(snapshot)); err != nil {
		return 0, err
	}
	n.restoreTimers(snapshot.Timers)
	return snapshot.Position, nil
}

func (n *ClusterNode) applyEntry(ctx context.Context, nodeID ClusterNodeID, role ClusterRole, entry ClusterLogEntry, replay bool) (ClusterEgress, error) {
	if n.service == nil {
		return ClusterEgress{}, ErrClusterServiceUnavailable
	}
	entryType := clusterLogEntryType(entry.Type)
	switch entryType {
	case ClusterLogEntryTimerSchedule:
		n.applyTimerSchedule(entry)
		return clusterEgressFromEntry(entry, nil), nil
	case ClusterLogEntryTimerCancel:
		n.applyTimerCancel(entry.TimerID)
		return clusterEgressFromEntry(entry, nil), nil
	case ClusterLogEntryTimerFire:
		n.applyTimerFire(entry.TimerID)
	case ClusterLogEntryIngress, ClusterLogEntryServiceMessage:
	default:
		return ClusterEgress{}, ErrClusterLogEntryType
	}
	payload, err := n.service.OnClusterMessage(ctx, ClusterMessage{
		NodeID:        nodeID,
		Role:          role,
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

func normalizeClusterConfig(cfg ClusterConfig) (ClusterConfig, error) {
	if cfg.NodeID == 0 {
		cfg.NodeID = defaultClusterNodeID
	}
	if cfg.Mode == "" {
		if cfg.Learner != nil {
			cfg.Mode = ClusterModeLearner
		} else if cfg.Election != nil {
			cfg.Mode = ClusterModeAppointedLeader
		} else {
			cfg.Mode = ClusterModeSingleNode
		}
	}
	switch cfg.Mode {
	case ClusterModeSingleNode:
		if cfg.Learner != nil {
			return ClusterConfig{}, invalidConfigf("learner config requires learner mode")
		}
		if cfg.Replication != nil {
			return ClusterConfig{}, invalidConfigf("replication config requires appointed-leader follower mode")
		}
		if cfg.Election != nil {
			return ClusterConfig{}, invalidConfigf("election config requires appointed-leader mode")
		}
		cfg.AppointedLeaderID = cfg.NodeID
	case ClusterModeAppointedLeader:
		if cfg.Learner != nil {
			return ClusterConfig{}, invalidConfigf("learner config requires learner mode")
		}
		if cfg.Election != nil {
			electionCfg, err := normalizeClusterElectionConfig(cfg.NodeID, cfg.AppointedLeaderID, cfg.Election)
			if err != nil {
				return ClusterConfig{}, err
			}
			cfg.Election = &electionCfg
			if cfg.AppointedLeaderID == 0 {
				cfg.AppointedLeaderID = electionCfg.initialLeaderID
			}
		}
		if cfg.AppointedLeaderID == 0 {
			return ClusterConfig{}, invalidConfigf("appointed leader id is required")
		}
		if cfg.Replication != nil {
			if cfg.NodeID == cfg.AppointedLeaderID {
				return ClusterConfig{}, invalidConfigf("replication config requires follower role")
			}
			replicationCfg, err := normalizeClusterReplicationConfig(cfg.Replication)
			if err != nil {
				return ClusterConfig{}, err
			}
			cfg.Replication = &replicationCfg
		}
	case ClusterModeLearner:
		if cfg.Replication != nil {
			return ClusterConfig{}, invalidConfigf("replication config requires appointed-leader follower mode")
		}
		if cfg.Election != nil {
			return ClusterConfig{}, invalidConfigf("election config requires appointed-leader mode")
		}
		learnerCfg, err := normalizeClusterLearnerConfig(cfg.Learner)
		if err != nil {
			return ClusterConfig{}, err
		}
		cfg.Learner = &learnerCfg
	default:
		return ClusterConfig{}, invalidConfigf("invalid cluster mode: %s", cfg.Mode)
	}
	if cfg.Log == nil {
		cfg.Log = NewInMemoryClusterLog()
	}
	if cfg.TimerCheckInterval < 0 {
		return ClusterConfig{}, invalidConfigf("invalid cluster timer check interval: %s", cfg.TimerCheckInterval)
	}
	if cfg.TimerCheckInterval == 0 {
		cfg.TimerCheckInterval = defaultClusterTimerCheckInterval
	}
	if cfg.Service == nil {
		return ClusterConfig{}, invalidConfigf("cluster service is required")
	}
	if cfg.Mode == ClusterModeLearner {
		if cfg.SnapshotStore == nil {
			return ClusterConfig{}, invalidConfigf("learner snapshot store is required")
		}
		if _, ok := cfg.Service.(ClusterSnapshotService); !ok {
			return ClusterConfig{}, invalidConfigf("learner service must support snapshots")
		}
	}
	return cfg, nil
}

func clusterRoleForConfig(cfg ClusterConfig) ClusterRole {
	if cfg.Mode == ClusterModeLearner {
		return ClusterRoleLearner
	}
	if cfg.NodeID == cfg.AppointedLeaderID {
		return ClusterRoleLeader
	}
	return ClusterRoleFollower
}

func cloneBytes(value []byte) []byte {
	return append([]byte(nil), value...)
}

func clusterLogEntryType(entryType ClusterLogEntryType) ClusterLogEntryType {
	if entryType == "" {
		return ClusterLogEntryIngress
	}
	return entryType
}

func clusterEgressFromEntry(entry ClusterLogEntry, payload []byte) ClusterEgress {
	return ClusterEgress{
		SessionID:     entry.SessionID,
		CorrelationID: entry.CorrelationID,
		LogPosition:   entry.Position,
		Type:          clusterLogEntryType(entry.Type),
		TimerID:       entry.TimerID,
		Deadline:      entry.Deadline,
		SourceService: entry.SourceService,
		TargetService: entry.TargetService,
		Payload:       cloneBytes(payload),
	}
}

func (n *ClusterNode) snapshotPosition(ctx context.Context) int64 {
	if n == nil || n.snapshotStore == nil {
		return 0
	}
	snapshot, ok, err := n.snapshotStore.Load(ctx)
	if err != nil || !ok {
		return 0
	}
	return snapshot.Position
}
