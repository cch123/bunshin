package bunshin

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestClusterElectionPromotesFollowerWhenLeaderHeartbeatExpires(t *testing.T) {
	timeout := 20 * time.Millisecond
	node, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            2,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		DisableTimerLoop:  true,
		Service: ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) {
			return []byte("ok"), nil
		}),
		Election: &ClusterElectionConfig{
			Members:          []ClusterNodeID{1, 2},
			HeartbeatTimeout: timeout,
			DisableAutoRun:   true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close(context.Background())

	base := time.Now().UTC()
	status, err := node.RecordHeartbeat(context.Background(), ClusterHeartbeat{
		NodeID:   1,
		LeaderID: 1,
		Term:     1,
		At:       base,
	})
	if err != nil {
		t.Fatal(err)
	}
	if status.LeaderID != 1 || status.Role != ClusterRoleFollower || node.Role() != ClusterRoleFollower {
		t.Fatalf("unexpected leader heartbeat status: %#v role=%q", status, node.Role())
	}
	status, err = node.TickElection(context.Background(), base.Add(timeout/2))
	if err != nil {
		t.Fatal(err)
	}
	if status.LeaderID != 1 || status.Role != ClusterRoleFollower {
		t.Fatalf("unexpected pre-timeout election status: %#v", status)
	}
	if _, err := node.Submit(context.Background(), ClusterIngress{Payload: []byte("before")}); !errors.Is(err, ErrClusterNotLeader) {
		t.Fatalf("Submit() before election err = %v, want %v", err, ErrClusterNotLeader)
	}

	status, err = node.TickElection(context.Background(), base.Add(timeout+time.Nanosecond))
	if err != nil {
		t.Fatal(err)
	}
	if status.LeaderID != 2 || status.Role != ClusterRoleLeader || status.Term != 2 || node.Role() != ClusterRoleLeader {
		t.Fatalf("unexpected post-timeout election status: %#v role=%q", status, node.Role())
	}
	egress, err := node.Submit(context.Background(), ClusterIngress{Payload: []byte("after")})
	if err != nil {
		t.Fatal(err)
	}
	if string(egress.Payload) != "ok" {
		t.Fatalf("unexpected egress after election: %#v", egress)
	}
}

func TestClusterElectionLeaderHeartbeatDemotesLocalLeader(t *testing.T) {
	node, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            2,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 2,
		DisableTimerLoop:  true,
		Service: ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) {
			return nil, nil
		}),
		Election: &ClusterElectionConfig{
			Members:        []ClusterNodeID{1, 2},
			DisableAutoRun: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close(context.Background())
	if node.Role() != ClusterRoleLeader {
		t.Fatalf("initial role = %q, want %q", node.Role(), ClusterRoleLeader)
	}

	status, err := node.RecordHeartbeat(context.Background(), ClusterHeartbeat{
		NodeID:   1,
		LeaderID: 1,
		Term:     2,
		At:       time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if status.LeaderID != 1 || status.Term != 2 || status.Role != ClusterRoleFollower || node.Role() != ClusterRoleFollower {
		t.Fatalf("unexpected demotion status: %#v role=%q", status, node.Role())
	}
	if _, err := node.Submit(context.Background(), ClusterIngress{Payload: []byte("after")}); !errors.Is(err, ErrClusterNotLeader) {
		t.Fatalf("Submit() after demotion err = %v, want %v", err, ErrClusterNotLeader)
	}
}

func TestClusterElectionStatusInDescribeAndValidate(t *testing.T) {
	node, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            2,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		DisableTimerLoop:  true,
		Service: ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) {
			return nil, nil
		}),
		Election: &ClusterElectionConfig{
			Members:        []ClusterNodeID{1, 2},
			DisableAutoRun: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close(context.Background())

	description, err := node.Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !description.Election.Enabled || description.Election.LeaderID != 1 ||
		description.Election.Role != ClusterRoleFollower || len(description.Election.Members) != 2 {
		t.Fatalf("unexpected election description: %#v", description)
	}
	report, err := node.Validate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !report.Healthy {
		t.Fatalf("unexpected validation report: %#v", report)
	}
}

func TestClusterElectionRejectsInvalidConfigAndHeartbeats(t *testing.T) {
	service := ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) {
		return nil, nil
	})
	if _, err := StartClusterNode(context.Background(), ClusterConfig{
		Mode:    ClusterModeSingleNode,
		Service: service,
		Election: &ClusterElectionConfig{
			Members: []ClusterNodeID{1, 2},
		},
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("single-node election err = %v, want %v", err, ErrInvalidConfig)
	}
	if _, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            2,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		Service:           service,
		Election: &ClusterElectionConfig{
			Members: []ClusterNodeID{1},
		},
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("election without local member err = %v, want %v", err, ErrInvalidConfig)
	}
	node, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            2,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		DisableTimerLoop:  true,
		Service:           service,
		Election: &ClusterElectionConfig{
			Members:        []ClusterNodeID{1, 2},
			DisableAutoRun: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close(context.Background())
	if _, err := node.RecordHeartbeat(context.Background(), ClusterHeartbeat{NodeID: 3}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unknown heartbeat err = %v, want %v", err, ErrInvalidConfig)
	}
}
