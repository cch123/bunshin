package core

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestClusterQuorumCommitsAfterMajorityAck(t *testing.T) {
	leaderLog := NewInMemoryClusterLog()
	followerOneLog := NewInMemoryClusterLog()
	followerTwoLog := NewInMemoryClusterLog()

	var delivered []ClusterMessage
	node, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            1,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		DisableTimerLoop:  true,
		Log:               leaderLog,
		Service: ClusterHandler(func(_ context.Context, msg ClusterMessage) ([]byte, error) {
			delivered = append(delivered, msg)
			return append([]byte("ack:"), msg.Payload...), nil
		}),
		Quorum: &ClusterQuorumConfig{
			MemberLogs: []ClusterLog{followerOneLog, followerTwoLog},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close(context.Background())

	egress, err := node.Submit(context.Background(), ClusterIngress{
		SessionID:     7,
		CorrelationID: 9,
		Payload:       []byte("one"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if egress.LogPosition != 1 || egress.SessionID != 7 || egress.CorrelationID != 9 ||
		string(egress.Payload) != "ack:one" {
		t.Fatalf("unexpected egress: %#v", egress)
	}
	if len(delivered) != 1 || delivered[0].LogPosition != 1 || delivered[0].Replay ||
		string(delivered[0].Payload) != "one" {
		t.Fatalf("unexpected delivered messages: %#v", delivered)
	}
	assertClusterLogEntry(t, "leader", leaderLog, 1, "one")
	assertClusterLogEntry(t, "follower one", followerOneLog, 1, "one")
	assertClusterLogEntry(t, "follower two", followerTwoLog, 1, "one")

	snapshot, err := node.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.Quorum.Enabled || snapshot.Quorum.Members != 3 || snapshot.Quorum.RequiredAck != 2 ||
		snapshot.Quorum.LastCommit != 1 || snapshot.Quorum.LastAcks != 3 || snapshot.Quorum.LastError != "" {
		t.Fatalf("unexpected quorum status: %#v", snapshot.Quorum)
	}
}

func TestClusterQuorumUnavailableDoesNotApplyOrAppendLocalLog(t *testing.T) {
	leaderLog := NewInMemoryClusterLog()
	reachableLog := NewInMemoryClusterLog()
	appendErr := errors.New("member unavailable")

	var delivered []ClusterMessage
	node, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            1,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		DisableTimerLoop:  true,
		Log:               leaderLog,
		Service: ClusterHandler(func(_ context.Context, msg ClusterMessage) ([]byte, error) {
			delivered = append(delivered, msg)
			return []byte("ack"), nil
		}),
		Quorum: &ClusterQuorumConfig{
			MemberLogs: []ClusterLog{
				reachableLog,
				failingClusterLog{err: appendErr},
				failingClusterLog{err: appendErr},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close(context.Background())

	_, err = node.Submit(context.Background(), ClusterIngress{
		SessionID:     7,
		CorrelationID: 9,
		Payload:       []byte("one"),
	})
	if !errors.Is(err, ErrClusterQuorumUnavailable) {
		t.Fatalf("Submit() err = %v, want %v", err, ErrClusterQuorumUnavailable)
	}
	if len(delivered) != 0 {
		t.Fatalf("service received uncommitted message: %#v", delivered)
	}
	position, err := leaderLog.LastPosition(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if position != 0 {
		t.Fatalf("leader log position = %d, want 0", position)
	}

	snapshot, err := node.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.Quorum.Enabled || snapshot.Quorum.LastCommit != 0 || snapshot.Quorum.LastAcks != 2 ||
		!strings.Contains(snapshot.Quorum.LastError, ErrClusterQuorumUnavailable.Error()) {
		t.Fatalf("unexpected quorum status after failed commit: %#v", snapshot.Quorum)
	}
	report, err := node.Validate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Healthy || len(report.Errors) == 0 ||
		!strings.Contains(report.Errors[0], ErrClusterQuorumUnavailable.Error()) {
		t.Fatalf("unexpected validation report: %#v", report)
	}
}

func TestClusterQuorumTreatsExistingMatchingMemberEntryAsAck(t *testing.T) {
	leaderLog := NewInMemoryClusterLog()
	followerLog := NewInMemoryClusterLog()
	if _, err := followerLog.Append(context.Background(), ClusterLogEntry{
		Position:      1,
		Type:          ClusterLogEntryIngress,
		SessionID:     7,
		CorrelationID: 9,
		Payload:       []byte("one"),
	}); err != nil {
		t.Fatal(err)
	}

	node, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            1,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		DisableTimerLoop:  true,
		Log:               leaderLog,
		Service: ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) {
			return []byte("ack"), nil
		}),
		Quorum: &ClusterQuorumConfig{
			MemberLogs:  []ClusterLog{followerLog},
			RequiredAck: 2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close(context.Background())

	egress, err := node.Submit(context.Background(), ClusterIngress{
		SessionID:     7,
		CorrelationID: 9,
		Payload:       []byte("one"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if egress.LogPosition != 1 || string(egress.Payload) != "ack" {
		t.Fatalf("unexpected egress: %#v", egress)
	}
	assertClusterLogEntry(t, "leader", leaderLog, 1, "one")
	entries, err := followerLog.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Position != 1 || string(entries[0].Payload) != "one" {
		t.Fatalf("unexpected follower log entries: %#v", entries)
	}
}

func TestClusterQuorumRejectsInvalidConfig(t *testing.T) {
	service := ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) {
		return nil, nil
	})
	if _, err := StartClusterNode(context.Background(), ClusterConfig{
		Mode:    ClusterModeSingleNode,
		Service: service,
		Quorum: &ClusterQuorumConfig{
			MemberLogs: []ClusterLog{NewInMemoryClusterLog()},
		},
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("single-node quorum err = %v, want %v", err, ErrInvalidConfig)
	}
	if _, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            2,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		Service:           service,
		Quorum: &ClusterQuorumConfig{
			MemberLogs: []ClusterLog{NewInMemoryClusterLog()},
		},
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("follower quorum err = %v, want %v", err, ErrInvalidConfig)
	}
	if _, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            1,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		Service:           service,
		Quorum: &ClusterQuorumConfig{
			MemberLogs:  []ClusterLog{NewInMemoryClusterLog()},
			RequiredAck: 1,
		},
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("low quorum required ack err = %v, want %v", err, ErrInvalidConfig)
	}
	if _, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            1,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		Service:           service,
		Quorum: &ClusterQuorumConfig{
			MemberLogs:  []ClusterLog{NewInMemoryClusterLog()},
			RequiredAck: 3,
		},
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("high quorum required ack err = %v, want %v", err, ErrInvalidConfig)
	}
}

func assertClusterLogEntry(t *testing.T, name string, log ClusterLog, position int64, payload string) {
	t.Helper()
	entries, err := log.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Position != position || string(entries[0].Payload) != payload {
		t.Fatalf("%s log entries = %#v, want position=%d payload=%q", name, entries, position, payload)
	}
}

type failingClusterLog struct {
	err error
}

func (l failingClusterLog) Append(context.Context, ClusterLogEntry) (ClusterLogEntry, error) {
	if l.err != nil {
		return ClusterLogEntry{}, l.err
	}
	return ClusterLogEntry{}, errors.New("cluster log append failed")
}

func (failingClusterLog) Snapshot(context.Context) ([]ClusterLogEntry, error) {
	return nil, nil
}

func (failingClusterLog) LastPosition(context.Context) (int64, error) {
	return 0, nil
}

func (failingClusterLog) Close() error {
	return nil
}
