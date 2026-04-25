package bunshin

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestArchiveControlServerRequests(t *testing.T) {
	archive, err := OpenArchive(ArchiveConfig{Path: filepath.Join(t.TempDir(), "archive")})
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()

	server, err := StartArchiveControlServer(archive, ArchiveControlConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client := server.Client()
	ctx := context.Background()

	descriptor, err := client.StartRecording(ctx, 10, 20)
	if err != nil {
		t.Fatal(err)
	}
	record, err := archive.Record(Message{StreamID: 10, SessionID: 20, Sequence: 1, Payload: []byte("one")})
	if err != nil {
		t.Fatal(err)
	}
	if record.RecordingID != descriptor.RecordingID {
		t.Fatalf("recording id = %d, want %d", record.RecordingID, descriptor.RecordingID)
	}

	queried, err := client.QueryRecording(ctx, descriptor.RecordingID)
	if err != nil {
		t.Fatal(err)
	}
	if queried.StopPosition != record.NextPosition {
		t.Fatalf("queried stop position = %d, want %d", queried.StopPosition, record.NextPosition)
	}
	recordings, err := client.ListRecordings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(recordings) != 1 || recordings[0].RecordingID != descriptor.RecordingID {
		t.Fatalf("unexpected recordings: %#v", recordings)
	}

	var replayed []Message
	result, err := client.Replay(ctx, ArchiveReplayConfig{RecordingID: descriptor.RecordingID}, func(_ context.Context, msg Message) error {
		replayed = append(replayed, msg)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Records != 1 || len(replayed) != 1 || string(replayed[0].Payload) != "one" {
		t.Fatalf("unexpected replay result=%#v messages=%#v", result, replayed)
	}

	if err := client.TruncateRecording(ctx, descriptor.RecordingID, record.Position); err != nil {
		t.Fatal(err)
	}
	report, err := client.IntegrityScan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.Records != 0 {
		t.Fatalf("records after truncate = %d, want 0", report.Records)
	}
	if err := client.Purge(ctx); err != nil {
		t.Fatal(err)
	}
	recordings, err = client.ListRecordings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(recordings) != 0 {
		t.Fatalf("recordings after purge: %#v", recordings)
	}
}

func TestArchiveControlServerRejectsAfterClose(t *testing.T) {
	archive := openTestArchive(t)
	defer archive.Close()

	server, err := StartArchiveControlServer(archive, ArchiveControlConfig{})
	if err != nil {
		t.Fatal(err)
	}
	client := server.Client()
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListRecordings(context.Background()); !errors.Is(err, ErrArchiveControlClosed) {
		t.Fatalf("ListRecordings() err = %v, want %v", err, ErrArchiveControlClosed)
	}
}
