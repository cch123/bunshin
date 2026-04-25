package bunshin

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"
)

func TestClusterStandbyBackupSyncsLogAppliesServiceAndSnapshots(t *testing.T) {
	sourceLog := NewInMemoryClusterLog()
	leader, err := StartClusterNode(context.Background(), ClusterConfig{
		Log:              sourceLog,
		DisableTimerLoop: true,
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

	backupLog := NewInMemoryClusterLog()
	store := NewInMemoryClusterSnapshotStore()
	service := &snapshotCounterService{}
	backup, err := StartClusterBackup(context.Background(), ClusterBackupConfig{
		NodeID:         2,
		SourceLog:      sourceLog,
		Log:            backupLog,
		SnapshotStore:  store,
		Service:        service,
		Standby:        true,
		DisableAutoRun: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer backup.Close()

	result, err := backup.Sync(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Copied != 2 || result.Applied != 2 || result.SyncedPosition != 2 ||
		!result.SnapshotTaken || result.Snapshot.Position != 2 {
		t.Fatalf("unexpected backup sync result: %#v", result)
	}
	if service.value != 2 {
		t.Fatalf("standby service value = %d, want 2", service.value)
	}
	snapshot, ok, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok || snapshot.Position != 2 || snapshot.Role != ClusterRoleStandby ||
		binary.BigEndian.Uint64(snapshot.Payload) != 2 {
		t.Fatalf("unexpected backup snapshot: %#v ok=%v", snapshot, ok)
	}
	position, err := backupLog.LastPosition(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if position != 2 {
		t.Fatalf("backup log position = %d, want 2", position)
	}

	recoveredService := &snapshotCounterService{}
	recovered, err := StartClusterNode(context.Background(), ClusterConfig{
		Log:              backupLog,
		SnapshotStore:    store,
		DisableTimerLoop: true,
		Service:          recoveredService,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close(context.Background())
	if recoveredService.value != 2 || recoveredService.loadedSnapshotPosition != 2 || recoveredService.replayed != 0 {
		t.Fatalf("unexpected recovered service: %#v", recoveredService)
	}
}

func TestClusterColdBackupCopiesLogWithoutApplyingService(t *testing.T) {
	sourceLog := NewInMemoryClusterLog()
	leader, err := StartClusterNode(context.Background(), ClusterConfig{
		Log:              sourceLog,
		DisableTimerLoop: true,
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

	backupLog := NewInMemoryClusterLog()
	service := &snapshotCounterService{}
	backup, err := StartClusterBackup(context.Background(), ClusterBackupConfig{
		SourceLog:      sourceLog,
		Log:            backupLog,
		Service:        service,
		DisableAutoRun: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer backup.Close()
	result, err := backup.Sync(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Copied != 1 || result.Applied != 0 || service.value != 0 {
		t.Fatalf("unexpected cold backup result=%#v service=%#v", result, service)
	}
	status, err := backup.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.Role != ClusterRoleBackup || status.Standby || status.SyncedPosition != 1 {
		t.Fatalf("unexpected cold backup status: %#v", status)
	}
}

func TestClusterBackupRestoresSnapshotAndContinuesSync(t *testing.T) {
	sourceLog := NewInMemoryClusterLog()
	leader, err := StartClusterNode(context.Background(), ClusterConfig{
		Log:              sourceLog,
		DisableTimerLoop: true,
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
	store := NewInMemoryClusterSnapshotStore()
	first, err := StartClusterBackup(context.Background(), ClusterBackupConfig{
		SourceLog:      sourceLog,
		SnapshotStore:  store,
		Service:        &snapshotCounterService{},
		Standby:        true,
		DisableAutoRun: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Send(context.Background(), []byte("increment")); err != nil {
		t.Fatal(err)
	}

	service := &snapshotCounterService{}
	secondLog := NewInMemoryClusterLog()
	second, err := StartClusterBackup(context.Background(), ClusterBackupConfig{
		SourceLog:      sourceLog,
		Log:            secondLog,
		SnapshotStore:  store,
		Service:        service,
		Standby:        true,
		DisableAutoRun: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	result, err := second.Sync(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.PreviousPosition != 2 || result.Copied != 1 || result.Applied != 1 || result.SyncedPosition != 3 {
		t.Fatalf("unexpected continued backup result: %#v", result)
	}
	if service.loadedSnapshotPosition != 2 || service.value != 3 {
		t.Fatalf("unexpected continued backup service: %#v", service)
	}
	entries, err := secondLog.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Position != 3 {
		t.Fatalf("unexpected continued backup log entries: %#v", entries)
	}
}

func TestClusterBackupAutoRunSyncsSourceLog(t *testing.T) {
	sourceLog := NewInMemoryClusterLog()
	leader, err := StartClusterNode(context.Background(), ClusterConfig{
		Log:              sourceLog,
		DisableTimerLoop: true,
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

	backupLog := NewInMemoryClusterLog()
	backup, err := StartClusterBackup(context.Background(), ClusterBackupConfig{
		SourceLog:    sourceLog,
		Log:          backupLog,
		SyncInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer backup.Close()
	if _, err := client.Send(context.Background(), []byte("increment")); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		position, err := backupLog.LastPosition(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if position == 1 {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("backup did not auto-sync source log")
		case <-tick.C:
		}
	}
}

func TestClusterBackupRejectsInvalidConfigAndClosedSync(t *testing.T) {
	if _, err := StartClusterBackup(context.Background(), ClusterBackupConfig{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("StartClusterBackup() err = %v, want %v", err, ErrInvalidConfig)
	}
	if _, err := StartClusterBackup(context.Background(), ClusterBackupConfig{
		SourceLog: NewInMemoryClusterLog(),
		Standby:   true,
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("standby without service err = %v, want %v", err, ErrInvalidConfig)
	}
	if _, err := StartClusterBackup(context.Background(), ClusterBackupConfig{
		SourceLog:     NewInMemoryClusterLog(),
		SnapshotStore: NewInMemoryClusterSnapshotStore(),
		Service:       ClusterHandler(func(context.Context, ClusterMessage) ([]byte, error) { return nil, nil }),
	}); !errors.Is(err, ErrClusterBackupUnsupported) {
		t.Fatalf("snapshot unsupported err = %v, want %v", err, ErrClusterBackupUnsupported)
	}
	backup, err := StartClusterBackup(context.Background(), ClusterBackupConfig{
		SourceLog:      NewInMemoryClusterLog(),
		DisableAutoRun: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := backup.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := backup.Sync(context.Background()); !errors.Is(err, ErrClusterBackupClosed) {
		t.Fatalf("closed Sync() err = %v, want %v", err, ErrClusterBackupClosed)
	}
}
