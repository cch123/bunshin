package bunshin

import (
	"context"
	"encoding/binary"
	"testing"
	"time"
)

func TestClusterPartitionFailoverSnapshotRecoveryAndBackup(t *testing.T) {
	sourceLog := NewInMemoryClusterLog()
	leader, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            1,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		Log:               sourceLog,
		DisableTimerLoop:  true,
		Service:           &snapshotCounterService{},
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
	followerStore := NewInMemoryClusterSnapshotStore()
	followerService := &snapshotCounterService{}
	timeout := 20 * time.Millisecond
	follower, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            2,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 1,
		Log:               followerLog,
		SnapshotStore:     followerStore,
		DisableTimerLoop:  true,
		Service:           followerService,
		Replication: &ClusterReplicationConfig{
			SourceLog:      sourceLog,
			DisableAutoRun: true,
		},
		Election: &ClusterElectionConfig{
			Members:          []ClusterNodeID{1, 2},
			HeartbeatTimeout: timeout,
			DisableAutoRun:   true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close(context.Background())

	syncResult, err := follower.SyncReplication(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if syncResult.Applied != 2 || syncResult.SyncedPosition != 2 || followerService.value != 2 {
		t.Fatalf("unexpected follower replication result=%#v service=%#v", syncResult, followerService)
	}
	base := time.Now().UTC()
	if _, err := follower.RecordHeartbeat(context.Background(), ClusterHeartbeat{
		NodeID:   1,
		LeaderID: 1,
		Term:     1,
		At:       base,
	}); err != nil {
		t.Fatal(err)
	}
	status, err := follower.TickElection(context.Background(), base.Add(timeout+time.Nanosecond))
	if err != nil {
		t.Fatal(err)
	}
	if status.LeaderID != 2 || status.Role != ClusterRoleLeader || follower.Role() != ClusterRoleLeader {
		t.Fatalf("follower did not fail over after partition: status=%#v role=%q", status, follower.Role())
	}
	egress, err := follower.Submit(context.Background(), ClusterIngress{Payload: []byte("increment")})
	if err != nil {
		t.Fatal(err)
	}
	if egress.LogPosition != 3 || followerService.value != 3 {
		t.Fatalf("unexpected failover submit egress=%#v service=%#v", egress, followerService)
	}
	snapshot, err := follower.TakeSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Position != 3 || binary.BigEndian.Uint64(snapshot.Payload) != 3 {
		t.Fatalf("unexpected failover snapshot: %#v", snapshot)
	}

	recoveredService := &snapshotCounterService{}
	recovered, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:            2,
		Mode:              ClusterModeAppointedLeader,
		AppointedLeaderID: 2,
		Log:               followerLog,
		SnapshotStore:     followerStore,
		DisableTimerLoop:  true,
		Service:           recoveredService,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close(context.Background())
	if recoveredService.loadedSnapshotPosition != 3 || recoveredService.value != 3 || recoveredService.replayed != 0 {
		t.Fatalf("unexpected recovered service: %#v", recoveredService)
	}

	backupStore := NewInMemoryClusterSnapshotStore()
	backupService := &snapshotCounterService{}
	backup, err := StartClusterBackup(context.Background(), ClusterBackupConfig{
		NodeID:         3,
		SourceLog:      followerLog,
		Log:            NewInMemoryClusterLog(),
		SnapshotStore:  backupStore,
		Service:        backupService,
		Standby:        true,
		DisableAutoRun: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer backup.Close()
	backupResult, err := backup.Sync(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if backupResult.Copied != 3 || backupResult.Applied != 3 || backupResult.SyncedPosition != 3 || backupService.value != 3 {
		t.Fatalf("unexpected backup sync result=%#v service=%#v", backupResult, backupService)
	}
	backupSnapshot, ok, err := backupStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok || backupSnapshot.Position != 3 || backupSnapshot.Role != ClusterRoleStandby {
		t.Fatalf("unexpected backup snapshot: %#v ok=%v", backupSnapshot, ok)
	}
}
