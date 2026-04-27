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
	if egress.LogPosition != 1 || binary.BigEndian.Uint64(egress.Payload) != 1 || leaderService.currentValue() != 1 {
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
	if result.SourcePosition != 1 || result.SyncedPosition != 1 || result.Applied != 1 || followerService.currentValue() != 1 {
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

func TestClusterMemberTransportQuorumBatchAppend(t *testing.T) {
	followerLog := NewInMemoryClusterLog()
	follower, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            2,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		DisableTimerLoop:  true,
		Log:               followerLog,
		Service:           clusterBenchmarkNoopService{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close(context.Background())
	server, remoteFollower := startClusterMemberTransportForTest(t, follower)
	defer server.Close()
	defer remoteFollower.Close()

	leader, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            1,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		DisableTimerLoop:  true,
		Service:           clusterBenchmarkNoopService{},
		Quorum: &ClusterQuorumConfig{
			MemberLogs:  []ClusterLog{remoteFollower},
			RequiredAck: 2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close(context.Background())
	client, err := leader.NewClient(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	egresses, err := client.SendBatch(context.Background(), [][]byte{
		[]byte("one"),
		[]byte("two"),
		[]byte("three"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(egresses) != 3 || egresses[0].LogPosition != 1 || egresses[2].LogPosition != 3 {
		t.Fatalf("unexpected batch quorum egresses: %#v", egresses)
	}
	entries, err := followerLog.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 || entries[0].Position != 1 || string(entries[0].Payload) != "one" ||
		entries[1].Position != 2 || string(entries[1].Payload) != "two" ||
		entries[2].Position != 3 || string(entries[2].Payload) != "three" {
		t.Fatalf("unexpected follower batch entries: %#v", entries)
	}
}

func TestClusterMemberTransportSubmitsIngressBatch(t *testing.T) {
	service := &snapshotCounterService{}
	node, err := StartClusterNode(context.Background(), ClusterConfig{
		DisableTimerLoop: true,
		Service:          service,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close(context.Background())
	server, remote := startClusterMemberTransportForTest(t, node)
	defer server.Close()
	defer remote.Close()

	egresses, err := remote.SubmitBatch(context.Background(), []ClusterIngress{
		{SessionID: 7, CorrelationID: 1, Payload: []byte("increment")},
		{SessionID: 7, CorrelationID: 2, Payload: []byte("increment")},
		{SessionID: 7, CorrelationID: 3, Payload: []byte("increment")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(egresses) != 3 || egresses[0].LogPosition != 1 || egresses[1].LogPosition != 2 ||
		egresses[2].LogPosition != 3 || service.currentValue() != 3 {
		t.Fatalf("unexpected remote batch egresses=%#v service=%#v", egresses, service)
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

func TestClusterMemberTransportSupportsExplicitTCP(t *testing.T) {
	node, err := StartClusterNode(context.Background(), ClusterConfig{
		DisableTimerLoop: true,
		Service: ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) {
			return []byte("ack"), nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close(context.Background())

	server, err := ListenClusterMemberTransport(context.Background(), node, ClusterMemberTransportConfig{
		Network: "tcp",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client, err := DialClusterMember(context.Background(), ClusterMemberClientConfig{
		Network: "tcp",
		Addr:    server.Addr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	egress, err := client.Submit(context.Background(), ClusterIngress{Payload: []byte("one")})
	if err != nil {
		t.Fatal(err)
	}
	if egress.LogPosition != 1 || string(egress.Payload) != "ack" {
		t.Fatalf("unexpected TCP egress: %#v", egress)
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
	if failoverService.currentValue() != 1 || failoverService.replayed != 1 {
		t.Fatalf("unexpected failover replay service: %#v", failoverService)
	}
	egress, err := newLeader.Submit(context.Background(), ClusterIngress{Payload: []byte("increment")})
	if err != nil {
		t.Fatal(err)
	}
	if egress.LogPosition != 2 || binary.BigEndian.Uint64(egress.Payload) != 2 || failoverService.currentValue() != 2 {
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
	if result.SourcePosition != 2 || result.SyncedPosition != 2 || result.Applied != 2 || staleService.currentValue() != 2 {
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
