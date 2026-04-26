package bunshin

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrClusterQuorumUnavailable = errors.New("bunshin cluster quorum: unavailable")

type ClusterQuorumConfig struct {
	MemberLogs  []ClusterLog
	RequiredAck int
}

type ClusterQuorumStatus struct {
	Enabled     bool      `json:"enabled"`
	Members     int       `json:"members"`
	RequiredAck int       `json:"required_ack"`
	LastCommit  int64     `json:"last_commit"`
	LastAcks    int       `json:"last_acks"`
	LastError   string    `json:"last_error,omitempty"`
	LastAt      time.Time `json:"last_at,omitempty"`
}

type clusterQuorumState struct {
	memberLogs  []ClusterLog
	requiredAck int
	lastCommit  int64
	lastAcks    int
	lastError   string
	lastAt      time.Time
}

func normalizeClusterQuorumConfig(cfg *ClusterQuorumConfig) (ClusterQuorumConfig, error) {
	if cfg == nil {
		return ClusterQuorumConfig{}, invalidConfigf("quorum config is required")
	}
	normalized := *cfg
	if len(normalized.MemberLogs) == 0 {
		return ClusterQuorumConfig{}, invalidConfigf("cluster quorum member logs are required")
	}
	for i, log := range normalized.MemberLogs {
		if log == nil {
			return ClusterQuorumConfig{}, invalidConfigf("cluster quorum member log %d is required", i)
		}
	}
	totalMembers := 1 + len(normalized.MemberLogs)
	majority := totalMembers/2 + 1
	if normalized.RequiredAck == 0 {
		normalized.RequiredAck = majority
	}
	if normalized.RequiredAck < majority || normalized.RequiredAck > totalMembers {
		return ClusterQuorumConfig{}, invalidConfigf("invalid cluster quorum required ack: %d", normalized.RequiredAck)
	}
	return normalized, nil
}

func newClusterQuorumState(cfg *ClusterQuorumConfig) *clusterQuorumState {
	if cfg == nil {
		return nil
	}
	return &clusterQuorumState{
		memberLogs:  append([]ClusterLog(nil), cfg.MemberLogs...),
		requiredAck: cfg.RequiredAck,
	}
}

func (n *ClusterNode) appendClusterEntry(ctx context.Context, entry ClusterLogEntry) (ClusterLogEntry, error) {
	n.mu.Lock()
	quorum := n.quorum
	n.mu.Unlock()
	if quorum == nil {
		return n.log.Append(ctx, entry)
	}
	return n.appendClusterEntryWithQuorum(ctx, quorum, entry)
}

func (n *ClusterNode) appendClusterEntryWithQuorum(ctx context.Context, quorum *clusterQuorumState, entry ClusterLogEntry) (ClusterLogEntry, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	position, err := n.log.LastPosition(ctx)
	if err != nil {
		n.recordQuorumCommit(0, 0, err)
		return ClusterLogEntry{}, err
	}
	entry = cloneClusterLogEntry(entry)
	entry.Position = position + 1

	acks := 1
	var commitErr error
	for _, memberLog := range quorum.memberLogs {
		if err := appendClusterQuorumMember(ctx, memberLog, entry); err != nil {
			commitErr = errors.Join(commitErr, err)
			continue
		}
		acks++
	}
	if acks < quorum.requiredAck {
		err := fmt.Errorf("%w: position=%d acks=%d required=%d", ErrClusterQuorumUnavailable, entry.Position, acks, quorum.requiredAck)
		if commitErr != nil {
			err = fmt.Errorf("%w: %w", err, commitErr)
		}
		n.recordQuorumCommit(entry.Position, acks, err)
		return ClusterLogEntry{}, err
	}

	appended, err := n.log.Append(ctx, entry)
	if err != nil {
		n.recordQuorumCommit(entry.Position, acks, err)
		return ClusterLogEntry{}, err
	}
	n.recordQuorumCommit(appended.Position, acks, nil)
	return appended, nil
}

func appendClusterQuorumMember(ctx context.Context, log ClusterLog, entry ClusterLogEntry) error {
	appended, err := log.Append(ctx, cloneClusterLogEntry(entry))
	if err == nil {
		if appended.Position != entry.Position || !sameClusterLogEntry(appended, entry) {
			return fmt.Errorf("%w: quorum member appended mismatched entry at position %d", ErrClusterLogPosition, appended.Position)
		}
		return nil
	}
	if errors.Is(err, ErrClusterLogPosition) {
		ok, lookupErr := clusterLogContainsEntry(ctx, log, entry)
		if lookupErr != nil {
			return lookupErr
		}
		if ok {
			return nil
		}
	}
	return err
}

func clusterLogContainsEntry(ctx context.Context, log ClusterLog, entry ClusterLogEntry) (bool, error) {
	entries, err := log.Snapshot(ctx)
	if err != nil {
		return false, err
	}
	for _, existing := range entries {
		if existing.Position == entry.Position {
			return sameClusterLogEntry(existing, entry), nil
		}
	}
	return false, nil
}

func sameClusterLogEntry(a, b ClusterLogEntry) bool {
	if a.Position != b.Position || clusterLogEntryType(a.Type) != clusterLogEntryType(b.Type) ||
		a.SessionID != b.SessionID || a.CorrelationID != b.CorrelationID ||
		a.TimerID != b.TimerID || !a.Deadline.Equal(b.Deadline) ||
		a.SourceService != b.SourceService || a.TargetService != b.TargetService ||
		!bytes.Equal(a.Payload, b.Payload) {
		return false
	}
	return true
}

func (n *ClusterNode) recordQuorumCommit(position int64, acks int, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.quorum == nil {
		return
	}
	n.quorum.lastAcks = acks
	n.quorum.lastAt = time.Now().UTC()
	if err != nil {
		n.quorum.lastError = err.Error()
		return
	}
	n.quorum.lastCommit = position
	n.quorum.lastError = ""
}

func (n *ClusterNode) quorumStatusLocked() ClusterQuorumStatus {
	if n.quorum == nil {
		return ClusterQuorumStatus{}
	}
	return ClusterQuorumStatus{
		Enabled:     true,
		Members:     1 + len(n.quorum.memberLogs),
		RequiredAck: n.quorum.requiredAck,
		LastCommit:  n.quorum.lastCommit,
		LastAcks:    n.quorum.lastAcks,
		LastError:   n.quorum.lastError,
		LastAt:      n.quorum.lastAt,
	}
}
