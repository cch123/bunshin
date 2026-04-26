package bunshin

import (
	"context"
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"
)

func TestArchiveClusterLogPersistsEntriesAcrossRestart(t *testing.T) {
	archive, err := OpenArchive(ArchiveConfig{Path: filepath.Join(t.TempDir(), "archive")})
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	log, err := NewArchiveClusterLog(archive)
	if err != nil {
		t.Fatal(err)
	}
	first, err := log.Append(context.Background(), ClusterLogEntry{Payload: []byte("one")})
	if err != nil {
		t.Fatal(err)
	}
	if first.Position != 1 {
		t.Fatalf("first position = %d, want 1", first.Position)
	}
	second, err := log.Append(context.Background(), ClusterLogEntry{Payload: []byte("two")})
	if err != nil {
		t.Fatal(err)
	}
	if second.Position != 2 {
		t.Fatalf("second position = %d, want 2", second.Position)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedLog, err := NewArchiveClusterLog(archive)
	if err != nil {
		t.Fatal(err)
	}
	position, err := reopenedLog.LastPosition(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if position != 2 {
		t.Fatalf("last position = %d, want 2", position)
	}
	entries, err := reopenedLog.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Position != 1 || string(entries[0].Payload) != "one" ||
		entries[1].Position != 2 || string(entries[1].Payload) != "two" {
		t.Fatalf("unexpected archive-backed log entries: %#v", entries)
	}
}

func TestArchiveClusterLogRejectsPositionGap(t *testing.T) {
	archive := openTestArchive(t)
	defer archive.Close()
	log, err := NewArchiveClusterLog(archive)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(context.Background(), ClusterLogEntry{Position: 2, Payload: []byte("gap")}); !errors.Is(err, ErrClusterLogPosition) {
		t.Fatalf("Append() err = %v, want %v", err, ErrClusterLogPosition)
	}
}

func TestArchiveBackedClusterRecoversLogAndSnapshot(t *testing.T) {
	archive, err := OpenArchive(ArchiveConfig{Path: filepath.Join(t.TempDir(), "archive")})
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	log, err := NewArchiveClusterLog(archive)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewArchiveClusterSnapshotStore(archive)
	if err != nil {
		t.Fatal(err)
	}

	firstService := &snapshotCounterService{}
	first, err := StartClusterNode(context.Background(), ClusterConfig{
		Log:           log,
		SnapshotStore: store,
		Service:       firstService,
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := first.NewClient(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Send(context.Background(), []byte("increment")); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Send(context.Background(), []byte("increment")); err != nil {
		t.Fatal(err)
	}
	snapshot, err := first.TakeSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Position != 2 || binary.BigEndian.Uint64(snapshot.Payload) != 2 {
		t.Fatalf("unexpected archive-backed snapshot: %#v", snapshot)
	}
	if _, err := client.Send(context.Background(), []byte("increment")); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	reopenedLog, err := NewArchiveClusterLog(archive)
	if err != nil {
		t.Fatal(err)
	}
	reopenedStore, err := NewArchiveClusterSnapshotStore(archive)
	if err != nil {
		t.Fatal(err)
	}
	secondService := &snapshotCounterService{}
	second, err := StartClusterNode(context.Background(), ClusterConfig{
		Log:           reopenedLog,
		SnapshotStore: reopenedStore,
		Service:       secondService,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close(context.Background())

	if secondService.value != 3 || secondService.loadedSnapshotPosition != 2 || secondService.replayed != 1 ||
		secondService.startContext.SnapshotPosition != 2 || secondService.startContext.LastPosition != 3 {
		t.Fatalf("unexpected archive-backed recovered service: %#v", secondService)
	}
	loaded, ok, err := reopenedStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok || loaded.Position != 2 || binary.BigEndian.Uint64(loaded.Payload) != 2 {
		t.Fatalf("unexpected loaded archive-backed snapshot: %#v ok=%v", loaded, ok)
	}
}
