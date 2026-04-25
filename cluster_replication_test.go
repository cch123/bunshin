package bunshin

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestClusterFollowerReplicationSyncsLeaderLog(t *testing.T) {
	leaderLog := NewInMemoryClusterLog()
	leader, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            1,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		Log:               leaderLog,
		Service: ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) {
			return nil, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close(context.Background())
	client, err := leader.NewClient(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Send(context.Background(), []byte("increment")); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Send(context.Background(), []byte("increment")); err != nil {
		t.Fatal(err)
	}

	followerLog := NewInMemoryClusterLog()
	followerService := &snapshotCounterService{}
	follower, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            2,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		Log:               followerLog,
		Service:           followerService,
		Replication: &ClusterReplicationConfig{
			SourceLog:      leaderLog,
			DisableAutoRun: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close(context.Background())

	if follower.Role() != ClusterRoleFollower {
		t.Fatalf("follower role = %q, want %q", follower.Role(), ClusterRoleFollower)
	}
	if _, err := follower.Submit(context.Background(), ClusterIngress{Payload: []byte("increment")}); !errors.Is(err, ErrClusterNotLeader) {
		t.Fatalf("follower Submit() err = %v, want %v", err, ErrClusterNotLeader)
	}

	result, err := follower.SyncReplication(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Applied != 2 || result.PreviousPosition != 0 ||
		result.SyncedPosition != 2 || result.SourcePosition != 2 {
		t.Fatalf("unexpected replication result: %#v", result)
	}
	if followerService.value != 2 {
		t.Fatalf("follower service value = %d, want 2", followerService.value)
	}
	position, err := followerLog.LastPosition(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if position != 2 {
		t.Fatalf("follower log position = %d, want 2", position)
	}
	status, err := follower.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !status.Replication.Enabled || status.Replication.SyncedPosition != 2 ||
		status.Replication.SourcePosition != 2 || status.Replication.Applied != 2 {
		t.Fatalf("unexpected replication status: %#v", status)
	}
}

func TestClusterFollowerReplicationCatchesUpFromLocalLog(t *testing.T) {
	leaderLog := NewInMemoryClusterLog()
	leader, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            1,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		Log:               leaderLog,
		Service: ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) {
			return nil, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close(context.Background())
	client, err := leader.NewClient(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Send(context.Background(), []byte("increment")); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Send(context.Background(), []byte("increment")); err != nil {
		t.Fatal(err)
	}

	followerLog := NewInMemoryClusterLog()
	first, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            2,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		Log:               followerLog,
		Service:           &snapshotCounterService{},
		Replication: &ClusterReplicationConfig{
			SourceLog:      leaderLog,
			DisableAutoRun: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.SyncReplication(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Send(context.Background(), []byte("increment")); err != nil {
		t.Fatal(err)
	}

	secondService := &snapshotCounterService{}
	second, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            2,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		Log:               followerLog,
		Service:           secondService,
		Replication: &ClusterReplicationConfig{
			SourceLog:      leaderLog,
			DisableAutoRun: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close(context.Background())

	if secondService.value != 2 || secondService.replayed != 2 {
		t.Fatalf("unexpected restarted follower service before catch-up: %#v", secondService)
	}
	result, err := second.SyncReplication(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.PreviousPosition != 2 || result.Applied != 1 || result.SyncedPosition != 3 {
		t.Fatalf("unexpected follower catch-up result: %#v", result)
	}
	if secondService.value != 3 {
		t.Fatalf("follower service value after catch-up = %d, want 3", secondService.value)
	}
}

func TestClusterFollowerReplicationAutoRunSyncsLeaderLog(t *testing.T) {
	leaderLog := NewInMemoryClusterLog()
	leader, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            1,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		Log:               leaderLog,
		Service: ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) {
			return nil, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close(context.Background())
	client, err := leader.NewClient(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	followerLog := NewInMemoryClusterLog()
	follower, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            2,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		Log:               followerLog,
		Service:           &snapshotCounterService{},
		Replication: &ClusterReplicationConfig{
			SourceLog:    leaderLog,
			SyncInterval: 5 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close(context.Background())

	if _, err := client.Send(context.Background(), []byte("increment")); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		position, err := followerLog.LastPosition(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if position == 1 {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("follower did not auto-replicate leader log")
		case <-tick.C:
		}
	}
}

func TestClusterReplicationRejectsInvalidConfig(t *testing.T) {
	service := &snapshotCounterService{}
	if _, err := StartClusterNode(context.Background(), ClusterConfig{
		Service: service,
		Replication: &ClusterReplicationConfig{
			SourceLog: NewInMemoryClusterLog(),
		},
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("single-node replication err = %v, want %v", err, ErrInvalidConfig)
	}
	if _, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            1,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		Service:           service,
		Replication: &ClusterReplicationConfig{
			SourceLog: NewInMemoryClusterLog(),
		},
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("leader replication err = %v, want %v", err, ErrInvalidConfig)
	}
	if _, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            2,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		Service:           service,
		Replication:       &ClusterReplicationConfig{},
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("replication without source log err = %v, want %v", err, ErrInvalidConfig)
	}
	if _, err := StartClusterNode(context.Background(), ClusterConfig{
		Mode:          ClusterModeLearner,
		SnapshotStore: NewInMemoryClusterSnapshotStore(),
		Service:       service,
		Learner: &ClusterLearnerConfig{
			MasterLog: NewInMemoryClusterLog(),
		},
		Replication: &ClusterReplicationConfig{
			SourceLog: NewInMemoryClusterLog(),
		},
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("learner replication err = %v, want %v", err, ErrInvalidConfig)
	}
}
