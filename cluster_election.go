package bunshin

import (
	"context"
	"errors"
	"sort"
	"time"
)

const (
	defaultClusterHeartbeatTimeout = time.Second
	defaultClusterElectionInterval = 100 * time.Millisecond
)

var ErrClusterElectionUnavailable = errors.New("bunshin cluster election: unavailable")

type ClusterElectionConfig struct {
	Members          []ClusterNodeID
	HeartbeatTimeout time.Duration
	TickInterval     time.Duration
	DisableAutoRun   bool

	initialLeaderID ClusterNodeID
}

type ClusterHeartbeat struct {
	NodeID   ClusterNodeID `json:"node_id"`
	LeaderID ClusterNodeID `json:"leader_id,omitempty"`
	Term     uint64        `json:"term,omitempty"`
	At       time.Time     `json:"at"`
}

type ClusterElectionStatus struct {
	Enabled      bool                          `json:"enabled"`
	LeaderID     ClusterNodeID                 `json:"leader_id"`
	Term         uint64                        `json:"term"`
	Role         ClusterRole                   `json:"role"`
	Members      []ClusterElectionMemberStatus `json:"members,omitempty"`
	LastElection time.Time                     `json:"last_election,omitempty"`
	LastError    string                        `json:"last_error,omitempty"`
}

type ClusterElectionMemberStatus struct {
	NodeID        ClusterNodeID `json:"node_id"`
	LastHeartbeat time.Time     `json:"last_heartbeat,omitempty"`
	Alive         bool          `json:"alive"`
}

type clusterElectionState struct {
	members          []ClusterNodeID
	memberSet        map[ClusterNodeID]struct{}
	heartbeats       map[ClusterNodeID]time.Time
	heartbeatTimeout time.Duration
	tickInterval     time.Duration
	disableAutoRun   bool
	leaderID         ClusterNodeID
	term             uint64
	lastElection     time.Time
	lastError        string
	cancel           context.CancelFunc
	done             chan struct{}
}

func (n *ClusterNode) RecordHeartbeat(ctx context.Context, heartbeat ClusterHeartbeat) (ClusterElectionStatus, error) {
	if n == nil {
		return ClusterElectionStatus{}, ErrClusterClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if heartbeat.At.IsZero() {
		heartbeat.At = time.Now().UTC()
	} else {
		heartbeat.At = heartbeat.At.UTC()
	}

	n.applyMu.Lock()

	n.mu.Lock()
	oldRole, newRole, status, err := n.recordHeartbeatLocked(heartbeat)
	n.mu.Unlock()

	n.applyMu.Unlock()
	n.applyElectionRoleTransition(oldRole, newRole)
	return status, err
}

func (n *ClusterNode) TickElection(ctx context.Context, now time.Time) (ClusterElectionStatus, error) {
	if n == nil {
		return ClusterElectionStatus{}, ErrClusterClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}

	n.applyMu.Lock()

	n.mu.Lock()
	oldRole, newRole, status, err := n.tickElectionLocked(now)
	n.mu.Unlock()

	n.applyMu.Unlock()
	n.applyElectionRoleTransition(oldRole, newRole)
	return status, err
}

func (n *ClusterNode) recordHeartbeatLocked(heartbeat ClusterHeartbeat) (ClusterRole, ClusterRole, ClusterElectionStatus, error) {
	if n.closed {
		return n.role, n.role, ClusterElectionStatus{}, ErrClusterClosed
	}
	election := n.election
	if election == nil {
		return n.role, n.role, ClusterElectionStatus{}, ErrClusterElectionUnavailable
	}
	if _, ok := election.memberSet[heartbeat.NodeID]; !ok {
		err := invalidConfigf("unknown cluster heartbeat member: %d", heartbeat.NodeID)
		election.lastError = err.Error()
		return n.role, n.role, election.status(n.nodeID, n.role, heartbeat.At), err
	}
	election.heartbeats[heartbeat.NodeID] = heartbeat.At
	if heartbeat.LeaderID != 0 {
		if _, ok := election.memberSet[heartbeat.LeaderID]; !ok {
			err := invalidConfigf("unknown cluster heartbeat leader: %d", heartbeat.LeaderID)
			election.lastError = err.Error()
			return n.role, n.role, election.status(n.nodeID, n.role, heartbeat.At), err
		}
	}
	if heartbeat.Term == 0 {
		heartbeat.Term = election.term
	}

	oldRole := n.role
	if heartbeat.LeaderID != 0 && heartbeat.NodeID == heartbeat.LeaderID && heartbeat.Term >= election.term {
		if election.leaderID != heartbeat.LeaderID || election.term != heartbeat.Term {
			election.lastElection = heartbeat.At
		}
		election.leaderID = heartbeat.LeaderID
		election.term = heartbeat.Term
	}
	n.role = roleForElectionLeader(n.nodeID, election.leaderID)
	election.lastError = ""
	return oldRole, n.role, election.status(n.nodeID, n.role, heartbeat.At), nil
}

func (n *ClusterNode) tickElectionLocked(now time.Time) (ClusterRole, ClusterRole, ClusterElectionStatus, error) {
	if n.closed {
		return n.role, n.role, ClusterElectionStatus{}, ErrClusterClosed
	}
	election := n.election
	if election == nil {
		return n.role, n.role, ClusterElectionStatus{}, ErrClusterElectionUnavailable
	}

	oldRole := n.role
	leaderID := election.leaderID
	if !election.memberAlive(n.nodeID, leaderID, now) {
		leaderID = election.firstAliveMember(n.nodeID, now)
		if leaderID != election.leaderID {
			election.leaderID = leaderID
			election.term++
			election.lastElection = now
		}
	}
	n.role = roleForElectionLeader(n.nodeID, election.leaderID)
	election.lastError = ""
	return oldRole, n.role, election.status(n.nodeID, n.role, now), nil
}

func normalizeClusterElectionConfig(nodeID, appointedLeaderID ClusterNodeID, cfg *ClusterElectionConfig) (ClusterElectionConfig, error) {
	if cfg == nil {
		return ClusterElectionConfig{}, invalidConfigf("election config is required")
	}
	normalized := *cfg
	members, err := normalizeClusterElectionMembers(normalized.Members)
	if err != nil {
		return ClusterElectionConfig{}, err
	}
	if _, ok := clusterNodeIDSet(members)[nodeID]; !ok {
		return ClusterElectionConfig{}, invalidConfigf("election members must include local node: %d", nodeID)
	}
	if appointedLeaderID != 0 {
		if _, ok := clusterNodeIDSet(members)[appointedLeaderID]; !ok {
			return ClusterElectionConfig{}, invalidConfigf("appointed leader must be an election member: %d", appointedLeaderID)
		}
		normalized.initialLeaderID = appointedLeaderID
	} else {
		normalized.initialLeaderID = members[0]
	}
	if normalized.HeartbeatTimeout < 0 {
		return ClusterElectionConfig{}, invalidConfigf("invalid cluster heartbeat timeout: %s", normalized.HeartbeatTimeout)
	}
	if normalized.HeartbeatTimeout == 0 {
		normalized.HeartbeatTimeout = defaultClusterHeartbeatTimeout
	}
	if normalized.TickInterval < 0 {
		return ClusterElectionConfig{}, invalidConfigf("invalid cluster election tick interval: %s", normalized.TickInterval)
	}
	if normalized.TickInterval == 0 {
		normalized.TickInterval = defaultClusterElectionInterval
	}
	normalized.Members = members
	return normalized, nil
}

func normalizeClusterElectionMembers(members []ClusterNodeID) ([]ClusterNodeID, error) {
	if len(members) == 0 {
		return nil, invalidConfigf("election members are required")
	}
	seen := make(map[ClusterNodeID]struct{}, len(members))
	normalized := make([]ClusterNodeID, 0, len(members))
	for _, member := range members {
		if member == 0 {
			return nil, invalidConfigf("election member id is required")
		}
		if _, ok := seen[member]; ok {
			continue
		}
		seen[member] = struct{}{}
		normalized = append(normalized, member)
	}
	sort.Slice(normalized, func(i, j int) bool {
		return normalized[i] < normalized[j]
	})
	return normalized, nil
}

func newClusterElectionState(cfg *ClusterElectionConfig, leaderID ClusterNodeID) *clusterElectionState {
	if cfg == nil {
		return nil
	}
	now := time.Now().UTC()
	heartbeats := make(map[ClusterNodeID]time.Time, len(cfg.Members))
	for _, member := range cfg.Members {
		heartbeats[member] = now
	}
	return &clusterElectionState{
		members:          append([]ClusterNodeID(nil), cfg.Members...),
		memberSet:        clusterNodeIDSet(cfg.Members),
		heartbeats:       heartbeats,
		heartbeatTimeout: cfg.HeartbeatTimeout,
		tickInterval:     cfg.TickInterval,
		disableAutoRun:   cfg.DisableAutoRun,
		leaderID:         leaderID,
		term:             1,
		lastElection:     now,
	}
}

func (n *ClusterNode) startElectionLoop() {
	n.mu.Lock()
	election := n.election
	if election == nil || election.disableAutoRun || election.cancel != nil {
		n.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	interval := election.tickInterval
	election.cancel = cancel
	election.done = done
	n.mu.Unlock()

	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				if _, err := n.TickElection(ctx, now); err != nil && !errors.Is(err, context.Canceled) {
					n.recordElectionError(err)
				}
			}
		}
	}()
}

func (n *ClusterNode) stopElectionLoop() {
	n.mu.Lock()
	var (
		cancel context.CancelFunc
		done   chan struct{}
	)
	if n.election != nil {
		cancel = n.election.cancel
		done = n.election.done
		n.election.cancel = nil
		n.election.done = nil
	}
	n.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (n *ClusterNode) applyElectionRoleTransition(oldRole, newRole ClusterRole) {
	if oldRole == newRole {
		return
	}
	if newRole == ClusterRoleLeader {
		n.stopReplicationLoop()
		n.startTimerLoop()
		return
	}
	if oldRole == ClusterRoleLeader {
		n.stopTimerLoop()
		n.startReplicationLoop()
	}
}

func (n *ClusterNode) recordElectionError(err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.election != nil && err != nil {
		n.election.lastError = err.Error()
	}
}

func (n *ClusterNode) electionStatusLocked(now time.Time) ClusterElectionStatus {
	if n.election == nil {
		return ClusterElectionStatus{}
	}
	return n.election.status(n.nodeID, n.role, now)
}

func (e *clusterElectionState) memberAlive(localNodeID, nodeID ClusterNodeID, now time.Time) bool {
	if nodeID == 0 {
		return false
	}
	if _, ok := e.memberSet[nodeID]; !ok {
		return false
	}
	if nodeID == localNodeID {
		return true
	}
	lastHeartbeat, ok := e.heartbeats[nodeID]
	if !ok || lastHeartbeat.IsZero() {
		return false
	}
	return !lastHeartbeat.Add(e.heartbeatTimeout).Before(now)
}

func (e *clusterElectionState) firstAliveMember(localNodeID ClusterNodeID, now time.Time) ClusterNodeID {
	for _, member := range e.members {
		if e.memberAlive(localNodeID, member, now) {
			return member
		}
	}
	return localNodeID
}

func (e *clusterElectionState) status(localNodeID ClusterNodeID, role ClusterRole, now time.Time) ClusterElectionStatus {
	members := make([]ClusterElectionMemberStatus, 0, len(e.members))
	for _, member := range e.members {
		members = append(members, ClusterElectionMemberStatus{
			NodeID:        member,
			LastHeartbeat: e.heartbeats[member],
			Alive:         e.memberAlive(localNodeID, member, now),
		})
	}
	return ClusterElectionStatus{
		Enabled:      true,
		LeaderID:     e.leaderID,
		Term:         e.term,
		Role:         role,
		Members:      members,
		LastElection: e.lastElection,
		LastError:    e.lastError,
	}
}

func roleForElectionLeader(localNodeID, leaderID ClusterNodeID) ClusterRole {
	if localNodeID == leaderID {
		return ClusterRoleLeader
	}
	return ClusterRoleFollower
}

func clusterNodeIDSet(ids []ClusterNodeID) map[ClusterNodeID]struct{} {
	set := make(map[ClusterNodeID]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return set
}
