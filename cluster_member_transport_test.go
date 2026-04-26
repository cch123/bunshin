package bunshin

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
)

func TestClusterMemberTransportSubmitsIngressAndReplicatesLog(t *testing.T) {
	leaderLog := NewInMemoryClusterLog()
	leaderService := &snapshotCounterService{}
	leader, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            1,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		DisableTimerLoop:  true,
		Log:               leaderLog,
		Service:           leaderService,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close(context.Background())

	server, remoteLeader := startClusterMemberTransportForTest(t, leader)
	defer server.Close()
	defer remoteLeader.Close()

	egress, err := remoteLeader.Submit(context.Background(), ClusterIngress{Payload: []byte("increment")})
	if err != nil {
		t.Fatal(err)
	}
	if egress.LogPosition != 1 || binary.BigEndian.Uint64(egress.Payload) != 1 || leaderService.value != 1 {
		t.Fatalf("unexpected remote submit result: egress=%#v leaderService=%#v", egress, leaderService)
	}

	followerLog := NewInMemoryClusterLog()
	followerService := &snapshotCounterService{}
	follower, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            2,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		DisableTimerLoop:  true,
		Log:               followerLog,
		Service:           followerService,
		Replication: &ClusterReplicationConfig{
			SourceLog:      remoteLeader,
			DisableAutoRun: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close(context.Background())

	result, err := follower.SyncReplication(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.SourcePosition != 1 || result.SyncedPosition != 1 || result.Applied != 1 || followerService.value != 1 {
		t.Fatalf("unexpected remote replication result=%#v followerService=%#v", result, followerService)
	}
}

func TestClusterMemberTransportQuorumAppendAndSnapshotTransfer(t *testing.T) {
	followerLog := NewInMemoryClusterLog()
	followerSnapshots := NewInMemoryClusterSnapshotStore()
	follower, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            2,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		DisableTimerLoop:  true,
		Log:               followerLog,
		SnapshotStore:     followerSnapshots,
		Service:           &snapshotCounterService{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close(context.Background())

	server, remoteFollower := startClusterMemberTransportForTest(t, follower)
	defer server.Close()
	defer remoteFollower.Close()

	leaderLog := NewInMemoryClusterLog()
	leader, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            1,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		DisableTimerLoop:  true,
		Log:               leaderLog,
		Service: ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) {
			return []byte("ack"), nil
		}),
		Quorum: &ClusterQuorumConfig{
			MemberLogs:  []ClusterLog{remoteFollower},
			RequiredAck: 2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close(context.Background())

	egress, err := leader.Submit(context.Background(), ClusterIngress{
		SessionID:     7,
		CorrelationID: 9,
		Payload:       []byte("one"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if egress.LogPosition != 1 || string(egress.Payload) != "ack" {
		t.Fatalf("unexpected quorum egress: %#v", egress)
	}
	position, err := followerLog.LastPosition(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if position != 1 {
		t.Fatalf("remote follower log position = %d, want 1", position)
	}

	snapshot := ClusterStateSnapshot{
		NodeID:   2,
		Role:     ClusterRoleFollower,
		Position: 1,
		Payload:  []byte("state"),
		Timers: []ClusterTimer{{
			TimerID: 3,
			Payload: []byte("timer"),
		}},
	}
	if err := remoteFollower.Save(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	loaded, ok, err := remoteFollower.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok || loaded.Position != 1 || string(loaded.Payload) != "state" ||
		len(loaded.Timers) != 1 || string(loaded.Timers[0].Payload) != "timer" {
		t.Fatalf("unexpected remote snapshot: %#v ok=%v", loaded, ok)
	}
}

func TestClusterMemberTransportMapsRemoteSentinelErrors(t *testing.T) {
	follower, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            2,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		DisableTimerLoop:  true,
		Service: ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) {
			return nil, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close(context.Background())

	server, remoteFollower := startClusterMemberTransportForTest(t, follower)
	defer server.Close()
	defer remoteFollower.Close()

	if _, err := remoteFollower.Submit(context.Background(), ClusterIngress{Payload: []byte("one")}); !errors.Is(err, ErrClusterNotLeader) {
		t.Fatalf("remote Submit() err = %v, want %v", err, ErrClusterNotLeader)
	}
	if _, _, err := remoteFollower.Load(context.Background()); !errors.Is(err, ErrClusterSnapshotStoreUnavailable) {
		t.Fatalf("remote Load() err = %v, want %v", err, ErrClusterSnapshotStoreUnavailable)
	}
}

func TestClusterMemberTransportQuorumFailoverAndCatchUp(t *testing.T) {
	followerOneLog := NewInMemoryClusterLog()
	followerOne, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            2,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		DisableTimerLoop:  true,
		Log:               followerOneLog,
		Service:           &snapshotCounterService{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer followerOne.Close(context.Background())
	followerOneServer, remoteFollowerOne := startClusterMemberTransportForTest(t, followerOne)
	defer followerOneServer.Close()
	defer remoteFollowerOne.Close()

	followerTwoLog := NewInMemoryClusterLog()
	followerTwo, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            3,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		DisableTimerLoop:  true,
		Log:               followerTwoLog,
		Service:           &snapshotCounterService{},
	})
	if err != nil {
		t.Fatal(err)
	}
	followerTwoServer, remoteFollowerTwo := startClusterMemberTransportForTest(t, followerTwo)

	leaderLog := NewInMemoryClusterLog()
	leader, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            1,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		DisableTimerLoop:  true,
		Log:               leaderLog,
		Service:           &snapshotCounterService{},
		Quorum: &ClusterQuorumConfig{
			MemberLogs: []ClusterLog{remoteFollowerOne, remoteFollowerTwo},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := leader.Submit(context.Background(), ClusterIngress{Payload: []byte("increment")}); err != nil {
		t.Fatal(err)
	}
	if err := leader.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := remoteFollowerTwo.Close(); err != nil {
		t.Fatal(err)
	}
	if err := followerTwoServer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := followerTwo.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	failoverService := &snapshotCounterService{}
	newLeader, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            3,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 3,
		DisableTimerLoop:  true,
		Log:               followerTwoLog,
		Service:           failoverService,
		Quorum: &ClusterQuorumConfig{
			MemberLogs:  []ClusterLog{remoteFollowerOne},
			RequiredAck: 2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer newLeader.Close(context.Background())
	if failoverService.value != 1 || failoverService.replayed != 1 {
		t.Fatalf("unexpected failover replay service: %#v", failoverService)
	}
	egress, err := newLeader.Submit(context.Background(), ClusterIngress{Payload: []byte("increment")})
	if err != nil {
		t.Fatal(err)
	}
	if egress.LogPosition != 2 || binary.BigEndian.Uint64(egress.Payload) != 2 || failoverService.value != 2 {
		t.Fatalf("unexpected failover egress=%#v service=%#v", egress, failoverService)
	}
	position, err := followerOneLog.LastPosition(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if position != 2 {
		t.Fatalf("remote quorum member position = %d, want 2", position)
	}

	newLeaderServer, remoteNewLeader := startClusterMemberTransportForTest(t, newLeader)
	defer newLeaderServer.Close()
	defer remoteNewLeader.Close()

	staleService := &snapshotCounterService{}
	stale, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            4,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 3,
		DisableTimerLoop:  true,
		Log:               NewInMemoryClusterLog(),
		Service:           staleService,
		Replication: &ClusterReplicationConfig{
			SourceLog:      remoteNewLeader,
			DisableAutoRun: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stale.Close(context.Background())

	result, err := stale.SyncReplication(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.SourcePosition != 2 || result.SyncedPosition != 2 || result.Applied != 2 || staleService.value != 2 {
		t.Fatalf("unexpected stale catch-up result=%#v service=%#v", result, staleService)
	}
}

func startClusterMemberTransportForTest(t *testing.T, node *ClusterNode) (*ClusterMemberTransportServer, *ClusterMemberClient) {
	t.Helper()
	server, err := ListenClusterMemberTransport(context.Background(), node, ClusterMemberTransportConfig{})
	if err != nil {
		t.Fatal(err)
	}
	client, err := DialClusterMember(context.Background(), ClusterMemberClientConfig{Addr: server.Addr().String()})
	if err != nil {
		_ = server.Close()
		t.Fatal(err)
	}
	return server, client
}
