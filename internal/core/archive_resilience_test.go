package core

import (
	"context"
	"path/filepath"
	"testing"
)

func TestArchiveCatalogExtensionReplayMergeAndReplication(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "src")
	archive, err := OpenArchive(ArchiveConfig{Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	first, err := archive.Record(Message{StreamID: 7, SessionID: 9, Sequence: 1, Payload: []byte("one")})
	if err != nil {
		t.Fatal(err)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenArchive(ArchiveConfig{Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	second, err := reopened.Record(Message{StreamID: 7, SessionID: 9, Sequence: 2, Payload: []byte("two")})
	if err != nil {
		t.Fatal(err)
	}
	if second.RecordingID != first.RecordingID || second.Position <= first.Position {
		t.Fatalf("recording extension did not append to existing recording: first=%#v second=%#v", first, second)
	}
	recordings, err := reopened.ListRecordings()
	if err != nil {
		t.Fatal(err)
	}
	if len(recordings) != 1 || recordings[0].RecordingID != first.RecordingID || recordings[0].StopPosition != second.NextPosition {
		t.Fatalf("unexpected catalog after extension: %#v", recordings)
	}

	var merged []string
	merge, err := reopened.NewReplayMerge(ArchiveReplayMergeConfig{
		Replay: ArchiveReplayConfig{RecordingID: first.RecordingID},
	}, func(_ context.Context, msg Message) error {
		merged = append(merged, string(msg.Payload))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	live := merge.LiveHandler()
	if err := live(context.Background(), Message{StreamID: 7, SessionID: 9, Sequence: 2, Payload: []byte("two-duplicate")}); err != nil {
		t.Fatal(err)
	}
	if err := live(context.Background(), Message{StreamID: 7, SessionID: 9, Sequence: 3, Payload: []byte("three")}); err != nil {
		t.Fatal(err)
	}
	result, err := merge.Replay(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Replayed != 2 || result.Live != 1 || result.BufferedLive != 2 || result.DroppedLive != 1 {
		t.Fatalf("unexpected replay merge result: %#v", result)
	}
	if len(merged) != 3 || merged[0] != "one" || merged[1] != "two" || merged[2] != "three" {
		t.Fatalf("unexpected replay merge delivery: %#v", merged)
	}

	if _, err := reopened.StopRecording(first.RecordingID); err != nil {
		t.Fatal(err)
	}
	dst, err := OpenArchive(ArchiveConfig{Path: filepath.Join(t.TempDir(), "dst")})
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	report, err := reopened.ReplicateTo(context.Background(), dst, ArchiveReplicationConfig{RecordingID: first.RecordingID})
	if err != nil {
		t.Fatal(err)
	}
	if report.SourceRecordingID != first.RecordingID || report.DestinationRecordingID == 0 || report.RecordingBytes != second.NextPosition {
		t.Fatalf("unexpected replication report: %#v", report)
	}
	var replayed []string
	if err := dst.Replay(context.Background(), ArchiveReplayConfig{RecordingID: report.DestinationRecordingID}, func(_ context.Context, msg Message) error {
		replayed = append(replayed, string(msg.Payload))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 2 || replayed[0] != "one" || replayed[1] != "two" {
		t.Fatalf("unexpected replicated replay: %#v", replayed)
	}
}
