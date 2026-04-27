package bunshin

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	defaultClusterNodeID ClusterNodeID = 1

	DefaultClusterClientMaxBatchSize  = 128
	DefaultClusterClientMaxBatchDelay = 50 * time.Microsecond
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
	ClusterRoleBackup   ClusterRole = "backup"
	ClusterRoleStandby  ClusterRole = "standby"
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
	Quorum             *ClusterQuorumConfig
	Authenticator      ClusterAuthenticator
	Authorizer         ClusterAuthorizer
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
	quorum             *clusterQuorumState
	authenticator      ClusterAuthenticator
	authorizer         ClusterAuthorizer
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
	Quorum           ClusterQuorumStatus
}

type ClusterClientConfig struct {
	MaxBatchSize  int
	MaxBatchDelay time.Duration
}

type ClusterClient struct {
	mu                sync.Mutex
	node              *ClusterNode
	sessionID         ClusterSessionID
	nextCorrelationID ClusterCorrelationID
	principal         ClusterPrincipal
	maxBatchSize      int
	maxBatchDelay     time.Duration
	batchQueue        []*clusterClientBatchRequest
	batchFlushActive  bool
	batchNotify       chan struct{}
}

type clusterClientBatchRequest struct {
	ctx     context.Context
	ingress ClusterIngress
	result  chan clusterClientBatchResult
}

type clusterClientBatchResult struct {
	egress ClusterEgress
	err    error
}

func DefaultClusterClientConfig() ClusterClientConfig {
	return ClusterClientConfig{
		MaxBatchSize:  DefaultClusterClientMaxBatchSize,
		MaxBatchDelay: DefaultClusterClientMaxBatchDelay,
	}
}

func normalizeClusterClientConfig(cfg ClusterClientConfig) (ClusterClientConfig, error) {
	if cfg.MaxBatchSize < 0 {
		return ClusterClientConfig{}, invalidConfigf("invalid cluster client max batch size: %d", cfg.MaxBatchSize)
	}
	if cfg.MaxBatchDelay < 0 {
		return ClusterClientConfig{}, invalidConfigf("invalid cluster client max batch delay: %s", cfg.MaxBatchDelay)
	}
	if cfg.MaxBatchSize <= 1 {
		cfg.MaxBatchSize = 1
		cfg.MaxBatchDelay = 0
	}
	return cfg, nil
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
		quorum:             newClusterQuorumState(normalized.Quorum),
		authenticator:      normalized.Authenticator,
		authorizer:         normalized.Authorizer,
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
		Quorum:      n.quorumStatusLocked(),
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
	return n.NewClientWithConfig(ctx, ClusterClientConfig{})
}

func (n *ClusterNode) NewClientWithConfig(ctx context.Context, cfg ClusterClientConfig) (*ClusterClient, error) {
	if n == nil {
		return nil, ErrClusterClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	cfg, err := normalizeClusterClientConfig(cfg)
	if err != nil {
		return nil, err
	}
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil, ErrClusterClosed
	}
	request := ClusterAuthenticationRequest{
		NodeID: n.nodeID,
		Role:   n.role,
	}
	n.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	principal, err := n.authenticateClusterPrincipal(ctx, request)
	if err != nil {
		return nil, err
	}
	if err := n.authorizeClusterAction(ctx, ClusterAuthorizationRequest{
		NodeID:    request.NodeID,
		Role:      request.Role,
		Principal: principal,
		Action:    ClusterAuthorizationActionOpenSession,
	}); err != nil {
		return nil, err
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil, ErrClusterClosed
	}
	sessionID := n.nextSessionID
	n.nextSessionID++
	return &ClusterClient{
		node:              n,
		sessionID:         sessionID,
		nextCorrelationID: 1,
		principal:         cloneClusterPrincipal(principal),
		maxBatchSize:      cfg.MaxBatchSize,
		maxBatchDelay:     cfg.MaxBatchDelay,
		batchNotify:       make(chan struct{}, 1),
	}, nil
}

func (n *ClusterNode) Submit(ctx context.Context, ingress ClusterIngress) (ClusterEgress, error) {
	return n.submit(ctx, ingress, clusterPrincipalFromContext(ctx))
}

func (n *ClusterNode) SubmitBatch(ctx context.Context, ingresses []ClusterIngress) ([]ClusterEgress, error) {
	return n.submitBatch(ctx, ingresses, clusterPrincipalFromContext(ctx))
}

func (n *ClusterNode) submit(ctx context.Context, ingress ClusterIngress, principal ClusterPrincipal) (ClusterEgress, error) {
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

	if err := n.authorizeClusterAction(ctx, ClusterAuthorizationRequest{
		NodeID:        nodeID,
		Role:          role,
		Principal:     principal,
		Action:        ClusterAuthorizationActionIngress,
		SessionID:     ingress.SessionID,
		CorrelationID: ingress.CorrelationID,
		Payload:       ingress.Payload,
	}); err != nil {
		return ClusterEgress{}, err
	}

	entry, err := n.appendClusterEntry(ctx, ClusterLogEntry{
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

func (n *ClusterNode) submitBatch(ctx context.Context, ingresses []ClusterIngress, principal ClusterPrincipal) ([]ClusterEgress, error) {
	if n == nil {
		return nil, ErrClusterClosed
	}
	if len(ingresses) == 0 {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	n.applyMu.Lock()
	defer n.applyMu.Unlock()

	normalized := make([]ClusterIngress, len(ingresses))
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil, ErrClusterClosed
	}
	if n.suspended {
		n.mu.Unlock()
		return nil, ErrClusterSuspended
	}
	if n.role != ClusterRoleLeader {
		n.mu.Unlock()
		return nil, ErrClusterNotLeader
	}
	for i, ingress := range ingresses {
		if ingress.SessionID == 0 {
			ingress.SessionID = n.nextSessionID
			n.nextSessionID++
		}
		if ingress.CorrelationID == 0 {
			ingress.CorrelationID = n.nextCorrelationID
			n.nextCorrelationID++
		}
		ingress.Payload = cloneBytes(ingress.Payload)
		normalized[i] = ingress
	}
	nodeID := n.nodeID
	role := n.role
	n.mu.Unlock()

	entries := make([]ClusterLogEntry, len(normalized))
	for i, ingress := range normalized {
		if err := n.authorizeClusterAction(ctx, ClusterAuthorizationRequest{
			NodeID:        nodeID,
			Role:          role,
			Principal:     principal,
			Action:        ClusterAuthorizationActionIngress,
			SessionID:     ingress.SessionID,
			CorrelationID: ingress.CorrelationID,
			Payload:       ingress.Payload,
		}); err != nil {
			return nil, err
		}
		entries[i] = ClusterLogEntry{
			Type:          ClusterLogEntryIngress,
			SessionID:     ingress.SessionID,
			CorrelationID: ingress.CorrelationID,
			Payload:       cloneBytes(ingress.Payload),
		}
	}

	appended, err := n.appendClusterEntries(ctx, entries)
	if err != nil {
		return nil, err
	}
	egresses := make([]ClusterEgress, len(appended))
	for i, entry := range appended {
		egress, err := n.applyEntry(ctx, nodeID, role, entry, false)
		if err != nil {
			return nil, err
		}
		egresses[i] = egress
	}
	return egresses, nil
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

	if err := n.authorizeClusterAction(ctx, ClusterAuthorizationRequest{
		Action: ClusterAuthorizationActionSnapshot,
	}); err != nil {
		return ClusterStateSnapshot{}, err
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
	if ctx == nil {
		ctx = context.Background()
	}
	c.mu.Lock()
	if c.maxBatchSize > 1 {
		correlationID := c.nextCorrelationID
		c.nextCorrelationID++
		request := &clusterClientBatchRequest{
			ctx: ctx,
			ingress: ClusterIngress{
				SessionID:     c.sessionID,
				CorrelationID: correlationID,
				Payload:       cloneBytes(payload),
			},
			result: make(chan clusterClientBatchResult, 1),
		}
		c.batchQueue = append(c.batchQueue, request)
		if !c.batchFlushActive {
			c.batchFlushActive = true
			go c.flushSendBatches()
		}
		c.notifyClusterClientBatchLocked()
		c.mu.Unlock()

		select {
		case result := <-request.result:
			return result.egress, result.err
		case <-ctx.Done():
			return ClusterEgress{}, ctx.Err()
		}
	}
	correlationID := c.nextCorrelationID
	c.nextCorrelationID++
	principal := cloneClusterPrincipal(c.principal)
	c.mu.Unlock()
	return c.node.submit(ctx, ClusterIngress{
		SessionID:     c.sessionID,
		CorrelationID: correlationID,
		Payload:       payload,
	}, principal)
}

func (c *ClusterClient) flushSendBatches() {
	for {
		if !c.waitClusterClientBatch() {
			return
		}
		requests, principal := c.takeClusterClientBatch()
		if len(requests) == 0 {
			continue
		}
		c.submitClusterClientBatch(requests, principal)
	}
}

func (c *ClusterClient) waitClusterClientBatch() bool {
	c.mu.Lock()
	if len(c.batchQueue) == 0 {
		c.batchFlushActive = false
		c.mu.Unlock()
		return false
	}
	maxBatchSize := c.maxBatchSize
	maxBatchDelay := c.maxBatchDelay
	if len(c.batchQueue) >= maxBatchSize || maxBatchDelay <= 0 {
		c.mu.Unlock()
		return true
	}
	notify := c.batchNotify
	c.mu.Unlock()

	timer := time.NewTimer(maxBatchDelay)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			return true
		case <-notify:
			c.mu.Lock()
			ready := len(c.batchQueue) >= maxBatchSize
			c.mu.Unlock()
			if ready {
				return true
			}
		}
	}
}

func (c *ClusterClient) takeClusterClientBatch() ([]*clusterClientBatchRequest, ClusterPrincipal) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.batchQueue) == 0 {
		return nil, ClusterPrincipal{}
	}
	batchSize := c.maxBatchSize
	if len(c.batchQueue) < batchSize {
		batchSize = len(c.batchQueue)
	}
	requests := make([]*clusterClientBatchRequest, batchSize)
	copy(requests, c.batchQueue[:batchSize])
	remaining := copy(c.batchQueue, c.batchQueue[batchSize:])
	for i := remaining; i < len(c.batchQueue); i++ {
		c.batchQueue[i] = nil
	}
	c.batchQueue = c.batchQueue[:remaining]
	return requests, cloneClusterPrincipal(c.principal)
}

func (c *ClusterClient) submitClusterClientBatch(requests []*clusterClientBatchRequest, principal ClusterPrincipal) {
	ingresses := make([]ClusterIngress, 0, len(requests))
	active := make([]*clusterClientBatchRequest, 0, len(requests))
	for _, request := range requests {
		if err := request.ctx.Err(); err != nil {
			request.result <- clusterClientBatchResult{err: err}
			continue
		}
		active = append(active, request)
		ingresses = append(ingresses, request.ingress)
	}
	if len(active) == 0 {
		return
	}

	egresses, err := c.node.submitBatch(clusterClientBatchContext(active), ingresses, principal)
	if err != nil {
		for _, request := range active {
			request.result <- clusterClientBatchResult{err: err}
		}
		return
	}
	if len(egresses) != len(active) {
		err := fmt.Errorf("%w: cluster client batch returned %d egresses for %d ingresses", ErrInvalidConfig, len(egresses), len(active))
		for _, request := range active {
			request.result <- clusterClientBatchResult{err: err}
		}
		return
	}
	for i, request := range active {
		request.result <- clusterClientBatchResult{egress: egresses[i]}
	}
}

func (c *ClusterClient) notifyClusterClientBatchLocked() {
	select {
	case c.batchNotify <- struct{}{}:
	default:
	}
}

func clusterClientBatchContext(requests []*clusterClientBatchRequest) context.Context {
	for _, request := range requests {
		if request.ctx != nil {
			return context.WithoutCancel(request.ctx)
		}
	}
	return context.Background()
}

func (c *ClusterClient) SendBatch(ctx context.Context, payloads [][]byte) ([]ClusterEgress, error) {
	if c == nil || c.node == nil {
		return nil, ErrClusterClosed
	}
	if len(payloads) == 0 {
		return nil, nil
	}
	c.mu.Lock()
	principal := cloneClusterPrincipal(c.principal)
	ingresses := make([]ClusterIngress, len(payloads))
	for i, payload := range payloads {
		correlationID := c.nextCorrelationID
		c.nextCorrelationID++
		ingresses[i] = ClusterIngress{
			SessionID:     c.sessionID,
			CorrelationID: correlationID,
			Payload:       payload,
		}
	}
	c.mu.Unlock()
	return c.node.submitBatch(ctx, ingresses, principal)
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
		if cfg.Quorum != nil {
			return ClusterConfig{}, invalidConfigf("quorum config requires appointed-leader mode")
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
		if cfg.Quorum != nil {
			if cfg.NodeID != cfg.AppointedLeaderID {
				return ClusterConfig{}, invalidConfigf("quorum config requires leader role")
			}
			quorumCfg, err := normalizeClusterQuorumConfig(cfg.Quorum)
			if err != nil {
				return ClusterConfig{}, err
			}
			cfg.Quorum = &quorumCfg
		}
	case ClusterModeLearner:
		if cfg.Replication != nil {
			return ClusterConfig{}, invalidConfigf("replication config requires appointed-leader follower mode")
		}
		if cfg.Election != nil {
			return ClusterConfig{}, invalidConfigf("election config requires appointed-leader mode")
		}
		if cfg.Quorum != nil {
			return ClusterConfig{}, invalidConfigf("quorum config requires appointed-leader mode")
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

func cloneClusterIngresses(ingresses []ClusterIngress) []ClusterIngress {
	if len(ingresses) == 0 {
		return nil
	}
	cloned := make([]ClusterIngress, len(ingresses))
	for i, ingress := range ingresses {
		ingress.Payload = cloneBytes(ingress.Payload)
		cloned[i] = ingress
	}
	return cloned
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
