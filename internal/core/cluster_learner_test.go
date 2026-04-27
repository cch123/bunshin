package core

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"
)

func TestClusterLearnerSyncsMasterLogAndSnapshots(t *testing.T) {
	masterLog := NewInMemoryClusterLog()
	master, err := StartClusterNode(context.Background(), ClusterConfig{
		Log: masterLog,
		Service: ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) {
			return nil, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close(context.Background())
	client, err := master.NewClient(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Send(context.Background(), []byte("increment")); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Send(context.Background(), []byte("increment")); err != nil {
		t.Fatal(err)
	}

	learnerLog := NewInMemoryClusterLog()
	learnerStore := NewInMemoryClusterSnapshotStore()
	learnerService := &snapshotCounterService{}
	learner, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:        2,
		Mode:          ClusterModeLearner,
		Log:           learnerLog,
		SnapshotStore: learnerStore,
		Service:       learnerService,
		Learner: &ClusterLearnerConfig{
			MasterLog:      masterLog,
			DisableAutoRun: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer learner.Close(context.Background())

	if learner.Role() != ClusterRoleLearner {
		t.Fatalf("learner role = %q, want %q", learner.Role(), ClusterRoleLearner)
	}
	if _, err := learner.Submit(context.Background(), ClusterIngress{Payload: []byte("increment")}); !errors.Is(err, ErrClusterNotLeader) {
		t.Fatalf("learner Submit() err = %v, want %v", err, ErrClusterNotLeader)
	}

	result, err := learner.SyncLearner(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Applied != 2 || result.PreviousPosition != 0 || result.SyncedPosition != 2 ||
		!result.SnapshotTaken || result.Snapshot.Position != 2 {
		t.Fatalf("unexpected learner sync result: %#v", result)
	}
	if learnerService.value != 2 {
		t.Fatalf("learner value = %d, want 2", learnerService.value)
	}
	position, err := learnerLog.LastPosition(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if position != 2 {
		t.Fatalf("learner log position = %d, want 2", position)
	}
	snapshot, ok, err := learnerStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok || snapshot.Position != 2 || binary.BigEndian.Uint64(snapshot.Payload) != 2 {
		t.Fatalf("unexpected learner snapshot: %#v ok=%v", snapshot, ok)
	}
	status, err := learner.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.Role != ClusterRoleLearner || status.Learner.SyncedPosition != 2 ||
		status.Learner.MasterPosition != 2 || status.Learner.Snapshots != 1 {
		t.Fatalf("unexpected learner status: %#v", status)
	}
}

func TestClusterLearnerRecoversFromSnapshotAndContinuesAtMasterPosition(t *testing.T) {
	masterLog := NewInMemoryClusterLog()
	master, err := StartClusterNode(context.Background(), ClusterConfig{
		Log: masterLog,
		Service: ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) {
			return nil, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close(context.Background())
	client, err := master.NewClient(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Send(context.Background(), []byte("increment")); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Send(context.Background(), []byte("increment")); err != nil {
		t.Fatal(err)
	}

	store := NewInMemoryClusterSnapshotStore()
	first, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:        2,
		Mode:          ClusterModeLearner,
		Log:           NewInMemoryClusterLog(),
		SnapshotStore: store,
		Service:       &snapshotCounterService{},
		Learner: &ClusterLearnerConfig{
			MasterLog:      masterLog,
			DisableAutoRun: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.SyncLearner(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Send(context.Background(), []byte("increment")); err != nil {
		t.Fatal(err)
	}

	learnerLog := NewInMemoryClusterLog()
	learnerService := &snapshotCounterService{}
	second, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:        2,
		Mode:          ClusterModeLearner,
		Log:           learnerLog,
		SnapshotStore: store,
		Service:       learnerService,
		Learner: &ClusterLearnerConfig{
			MasterLog:      masterLog,
			DisableAutoRun: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close(context.Background())

	result, err := second.SyncLearner(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.PreviousPosition != 2 || result.Applied != 1 || result.SyncedPosition != 3 {
		t.Fatalf("unexpected recovered learner sync result: %#v", result)
	}
	if learnerService.loadedSnapshotPosition != 2 || learnerService.value != 3 {
		t.Fatalf("unexpected recovered learner service: %#v", learnerService)
	}
	entries, err := learnerLog.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Position != 3 {
		t.Fatalf("unexpected recovered learner log entries: %#v", entries)
	}
}

func TestClusterLearnerAutoRunSyncsMasterLog(t *testing.T) {
	masterLog := NewInMemoryClusterLog()
	master, err := StartClusterNode(context.Background(), ClusterConfig{
		Log: masterLog,
		Service: ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) {
			return nil, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close(context.Background())
	client, err := master.NewClient(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	store := NewInMemoryClusterSnapshotStore()
	learner, err := StartClusterNode(context.Background(), ClusterConfig{
		NodeID:        2,
		Mode:          ClusterModeLearner,
		SnapshotStore: store,
		Service:       &snapshotCounterService{},
		Learner: &ClusterLearnerConfig{
			MasterLog:    masterLog,
			SyncInterval: 5 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer learner.Close(context.Background())

	if _, err := client.Send(context.Background(), []byte("increment")); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		snapshot, ok, err := store.Load(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if ok && snapshot.Position == 1 && binary.BigEndian.Uint64(snapshot.Payload) == 1 {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("learner did not auto-sync master log")
		case <-tick.C:
		}
	}
}

func TestClusterLearnerRejectsInvalidConfig(t *testing.T) {
	service := &snapshotCounterService{}
	if _, err := StartClusterNode(context.Background(), ClusterConfig{
		Mode:    ClusterModeLearner,
		Service: service,
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("learner without config err = %v, want %v", err, ErrInvalidConfig)
	}
	if _, err := StartClusterNode(context.Background(), ClusterConfig{
		Mode:          ClusterModeLearner,
		SnapshotStore: NewInMemoryClusterSnapshotStore(),
		Service:       service,
		Learner:       &ClusterLearnerConfig{},
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("learner without master log err = %v, want %v", err, ErrInvalidConfig)
	}
	if _, err := StartClusterNode(context.Background(), ClusterConfig{
		Mode:    ClusterModeLearner,
		Service: service,
		Learner: &ClusterLearnerConfig{
			MasterLog: NewInMemoryClusterLog(),
		},
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("learner without snapshot store err = %v, want %v", err, ErrInvalidConfig)
	}
	if _, err := StartClusterNode(context.Background(), ClusterConfig{
		Mode:          ClusterModeLearner,
		SnapshotStore: NewInMemoryClusterSnapshotStore(),
		Service:       ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) { return nil, nil }),
		Learner: &ClusterLearnerConfig{
			MasterLog: NewInMemoryClusterLog(),
		},
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("learner without snapshot service err = %v, want %v", err, ErrInvalidConfig)
	}
	if _, err := StartClusterNode(context.Background(), ClusterConfig{
		Mode:    ClusterModeSingleNode,
		Service: service,
		Learner: &ClusterLearnerConfig{
			MasterLog: NewInMemoryClusterLog(),
		},
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("non-learner with learner config err = %v, want %v", err, ErrInvalidConfig)
	}
}
