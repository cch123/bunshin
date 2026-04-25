package bunshin

import (
	"context"
	"errors"
	"sync"
)

const (
	defaultClusterNodeID ClusterNodeID = 1
)

var (
	ErrClusterClosed             = errors.New("bunshin cluster: closed")
	ErrClusterNotLeader          = errors.New("bunshin cluster: not leader")
	ErrClusterServiceUnavailable = errors.New("bunshin cluster: service unavailable")
	ErrClusterLogClosed          = errors.New("bunshin cluster log: closed")
)

type ClusterNodeID uint64

type ClusterSessionID uint64

type ClusterCorrelationID uint64

type ClusterMode string

const (
	ClusterModeSingleNode      ClusterMode = "single-node"
	ClusterModeAppointedLeader ClusterMode = "appointed-leader"
)

type ClusterRole string

const (
	ClusterRoleLeader   ClusterRole = "leader"
	ClusterRoleFollower ClusterRole = "follower"
)

type ClusterConfig struct {
	NodeID            ClusterNodeID
	Mode              ClusterMode
	AppointedLeaderID ClusterNodeID
	Log               ClusterLog
	Service           ClusterService
	Logger            Logger
}

type ClusterNode struct {
	mu                sync.Mutex
	applyMu           sync.Mutex
	nodeID            ClusterNodeID
	mode              ClusterMode
	role              ClusterRole
	log               ClusterLog
	service           ClusterService
	logger            Logger
	closed            bool
	nextSessionID     ClusterSessionID
	nextCorrelationID ClusterCorrelationID
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
	NodeID       ClusterNodeID
	Role         ClusterRole
	LastPosition int64
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
	Payload       []byte
}

type ClusterMessage struct {
	NodeID        ClusterNodeID
	Role          ClusterRole
	SessionID     ClusterSessionID
	CorrelationID ClusterCorrelationID
	LogPosition   int64
	Replay        bool
	Payload       []byte
}

type ClusterSnapshot struct {
	NodeID       ClusterNodeID
	Mode         ClusterMode
	Role         ClusterRole
	LastPosition int64
	Closed       bool
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
		nodeID:            normalized.NodeID,
		mode:              normalized.Mode,
		role:              clusterRoleForConfig(normalized),
		log:               normalized.Log,
		service:           normalized.Service,
		logger:            normalized.Logger,
		nextSessionID:     1,
		nextCorrelationID: 1,
	}
	if err := node.start(ctx); err != nil {
		return nil, err
	}
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
		NodeID: n.nodeID,
		Mode:   n.mode,
		Role:   n.role,
		Closed: n.closed,
	}
	n.mu.Unlock()

	position, err := n.log.LastPosition(ctx)
	if err != nil {
		return ClusterSnapshot{}, err
	}
	snapshot.LastPosition = position
	return snapshot, nil
}

func (n *ClusterNode) NewClient(ctx context.Context) (*ClusterClient, error) {
	if n == nil {
		return nil, ErrClusterClosed
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

func (n *ClusterNode) Close(ctx context.Context) error {
	if n == nil {
		return nil
	}
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
			NodeID:       nodeID,
			Role:         role,
			LastPosition: position,
		})
	}
	return nil
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

	if lifecycle, ok := n.service.(ClusterLifecycleService); ok {
		position, err := n.log.LastPosition(ctx)
		if err != nil {
			return err
		}
		if err := lifecycle.OnClusterStart(ctx, ClusterServiceContext{
			NodeID:       n.nodeID,
			Role:         n.role,
			LastPosition: position,
		}); err != nil {
			return err
		}
	}

	entries, err := n.log.Snapshot(ctx)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if _, err := n.applyEntry(ctx, n.nodeID, n.role, entry, true); err != nil {
			return err
		}
	}
	return nil
}

func (n *ClusterNode) applyEntry(ctx context.Context, nodeID ClusterNodeID, role ClusterRole, entry ClusterLogEntry, replay bool) (ClusterEgress, error) {
	if n.service == nil {
		return ClusterEgress{}, ErrClusterServiceUnavailable
	}
	payload, err := n.service.OnClusterMessage(ctx, ClusterMessage{
		NodeID:        nodeID,
		Role:          role,
		SessionID:     entry.SessionID,
		CorrelationID: entry.CorrelationID,
		LogPosition:   entry.Position,
		Replay:        replay,
		Payload:       cloneBytes(entry.Payload),
	})
	if err != nil {
		return ClusterEgress{}, err
	}
	return ClusterEgress{
		SessionID:     entry.SessionID,
		CorrelationID: entry.CorrelationID,
		LogPosition:   entry.Position,
		Payload:       cloneBytes(payload),
	}, nil
}

func normalizeClusterConfig(cfg ClusterConfig) (ClusterConfig, error) {
	if cfg.NodeID == 0 {
		cfg.NodeID = defaultClusterNodeID
	}
	if cfg.Mode == "" {
		cfg.Mode = ClusterModeSingleNode
	}
	switch cfg.Mode {
	case ClusterModeSingleNode:
		cfg.AppointedLeaderID = cfg.NodeID
	case ClusterModeAppointedLeader:
		if cfg.AppointedLeaderID == 0 {
			return ClusterConfig{}, invalidConfigf("appointed leader id is required")
		}
	default:
		return ClusterConfig{}, invalidConfigf("invalid cluster mode: %s", cfg.Mode)
	}
	if cfg.Log == nil {
		cfg.Log = NewInMemoryClusterLog()
	}
	if cfg.Service == nil {
		return ClusterConfig{}, invalidConfigf("cluster service is required")
	}
	return cfg, nil
}

func clusterRoleForConfig(cfg ClusterConfig) ClusterRole {
	if cfg.NodeID == cfg.AppointedLeaderID {
		return ClusterRoleLeader
	}
	return ClusterRoleFollower
}

func cloneBytes(value []byte) []byte {
	return append([]byte(nil), value...)
}
